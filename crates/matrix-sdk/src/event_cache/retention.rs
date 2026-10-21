// Hagency local changes to matrix-sdk 0.19.1; Apache-2.0.
//! A disposable SDK working cache. Authoritative history stays on the server;
//! Hagency's independently encrypted search index stores searchable history.
//! Disk chunks are bounded too, so OrderTracker never loads N historical links.
use matrix_sdk_base::{
    event_cache::{Event, store::EventCacheStoreLockGuard},
    linked_chunk::{
        ChunkContent, ChunkIdentifierGenerator, ChunkMetadata, LinkedChunkId, RawChunk, Update,
        lazy_loader,
    },
};

use super::{EventCacheError, Result, caches::event_linked_chunk::EventLinkedChunk};

pub(super) const MAX_EVENTS: usize = 512;
const KEEP_EVENTS: usize = 128;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 256 * 1024;
const MAX_CHUNKS: usize = 512;

// A local boundary, never a homeserver token. Persist it so a reopen cannot
// mistake a discarded prefix for the beginning of the authoritative history.
const WINDOW_BOUNDARY: &str = "hagency-internal:history-window-full:v1";

pub(super) fn check_pagination_token(token: &Option<String>) -> Result<()> {
    if token.as_deref() == Some(WINDOW_BOUNDARY) {
        return Err(EventCacheError::HistoryWindowFull);
    }
    Ok(())
}

pub(super) fn validate<'a>(events: impl Iterator<Item = &'a Event>) -> Result<()> {
    let mut count = 0;
    let mut bytes = 0;
    for event in events {
        let len = event.raw().json().get().len();
        if len > MAX_EVENT_BYTES {
            return Err(EventCacheError::HistoryWindowFull);
        }
        count += 1;
        bytes += len;
        if count > MAX_EVENTS || bytes > MAX_BYTES {
            return Err(EventCacheError::HistoryWindowFull);
        }
    }
    Ok(())
}

/// Bound side-loaded event batches before collecting them in memory.
pub(super) fn collect(events: impl IntoIterator<Item = Event>) -> Result<Vec<Event>> {
    let mut result = Vec::new();
    let mut bytes = 0usize;
    for event in events {
        validate(std::iter::once(&event))?;
        bytes += event.raw().json().get().len();
        if result.len() >= MAX_EVENTS || bytes > MAX_BYTES {
            return Err(EventCacheError::HistoryWindowFull);
        }
        result.push(event);
    }
    Ok(result)
}

pub(super) fn compact(chunk: &mut EventLinkedChunk) -> bool {
    let (ordered_items, ordered_chunks) = chunk.order_tracker.retained_metadata_size();
    if validate(chunk.events().map(|(_, event)| event)).is_ok()
        && ordered_items <= MAX_EVENTS
        && ordered_chunks <= MAX_CHUNKS
    {
        return false;
    }
    // A Clear plus append resets the SDK controller's positional/aggregation
    // metadata as well as its decoded items. Do not slice only the UI vector.
    let mut bytes = 0;
    let mut events: Vec<_> = chunk
        .revents()
        .take(KEEP_EVENTS)
        .take_while(|(_, event)| {
            let len = event.raw().json().get().len();
            bytes += len;
            len <= MAX_EVENT_BYTES && bytes <= MAX_BYTES
        })
        .map(|(_, event)| event.clone())
        .collect();
    events.reverse();
    chunk.reset();
    chunk.push_live_events(
        Some(matrix_sdk_base::event_cache::Gap { token: WINDOW_BOUNDARY.into() }),
        &events,
    );
    true
}

/// Never load the old full metadata vector, including on first upgrade from a
/// previously unbounded SDK cache. The store lock serializes this with sync.
pub(super) async fn load(
    store: &EventCacheStoreLockGuard,
    id: LinkedChunkId<'_>,
) -> Result<EventLinkedChunk> {
    let (last, generator, metadata) = load_snapshot(store, id).await?;
    let linked = lazy_loader::from_last_chunk(last, generator).map_err(EventCacheError::from)?;
    Ok(EventLinkedChunk::with_initial_linked_chunk(linked, metadata))
}

pub(super) type Snapshot = (
    Option<RawChunk<Event, matrix_sdk_base::event_cache::Gap>>,
    ChunkIdentifierGenerator,
    Option<Vec<ChunkMetadata>>,
);

/// Read at most a bounded chain, never the store's unbounded metadata vector.
/// Keep the original lazy representation and chunk IDs when it fits. Both
/// construction and cross-process/subscriber reload must use this path.
pub(super) async fn load_snapshot(
    store: &EventCacheStoreLockGuard,
    id: LinkedChunkId<'_>,
) -> Result<Snapshot> {
    let (last, generator) = match store.load_last_chunk(id).await {
        Ok(pair) => pair,
        Err(error) => {
            // This is a disposable working cache. Match the upstream recovery
            // path for malformed/cyclic chunks without loading all metadata.
            tracing::error!(?error, "Resetting an unreadable bounded history cache");
            store.handle_linked_chunk_updates(id, vec![Update::Clear]).await?;
            (None, ChunkIdentifierGenerator::new_from_scratch())
        }
    };
    let mut metadata = Vec::new();
    let mut count = 0;
    let mut bytes = 0;
    let mut cursor = last.clone();
    let mut fits = true;
    while let Some(raw) = cursor {
        if metadata.len() == MAX_CHUNKS
            || metadata.iter().any(|m: &ChunkMetadata| m.identifier == raw.identifier)
        {
            fits = false;
            break;
        }
        let num_items = match &raw.content {
            ChunkContent::Items(events) => {
                if events.len() > matrix_sdk_base::event_cache::store::DEFAULT_CHUNK_CAPACITY {
                    fits = false;
                    break;
                }
                for event in events {
                    let len = event.raw().json().get().len();
                    count += 1;
                    bytes += len;
                    if len > MAX_EVENT_BYTES || count > MAX_EVENTS || bytes > MAX_BYTES {
                        fits = false;
                        break;
                    }
                }
                events.len()
            }
            ChunkContent::Gap(_) => 0,
        };
        if !fits {
            break;
        }
        metadata.push(ChunkMetadata {
            num_items,
            previous: raw.previous,
            identifier: raw.identifier,
            next: raw.next,
        });
        if raw.previous.is_none() {
            break;
        }
        if metadata.len() == MAX_CHUNKS {
            fits = false;
            break;
        }
        cursor = match store.load_previous_chunk(id, raw.identifier).await {
            Ok(Some(previous))
                if Some(previous.identifier) == raw.previous
                    && previous.next == Some(raw.identifier) =>
            {
                Some(previous)
            }
            _ => {
                fits = false;
                break;
            }
        };
    }
    if fits {
        metadata.reverse();
        if matches!(&last, Some(raw) if raw.previous.is_none() && matches!(&raw.content, ChunkContent::Items(events) if events.is_empty()))
        {
            store.handle_linked_chunk_updates(id, vec![Update::Clear]).await?;
            return Ok((None, ChunkIdentifierGenerator::new_from_scratch(), None));
        }
        return Ok((last, generator, (!metadata.is_empty()).then_some(metadata)));
    }
    let (gap, events) = match last.map(|c| c.content) {
        Some(ChunkContent::Items(events)) => (None, events),
        Some(ChunkContent::Gap(gap)) => (Some(gap), Vec::new()),
        None => (None, Vec::new()),
    };
    // A legacy last chunk can exceed the new byte budget. Keep a bounded
    // suffix rather than making the room impossible to open on upgrade.
    let mut bytes = 0usize;
    let mut events: Vec<_> = events
        .into_iter()
        .rev()
        .take(KEEP_EVENTS)
        .take_while(|event| {
            let len = event.raw().json().get().len();
            bytes += len;
            len <= MAX_EVENT_BYTES && bytes <= MAX_BYTES
        })
        .collect();
    events.reverse();
    let mut chunk = EventLinkedChunk::new();
    chunk.push_live_events(
        gap.or_else(|| Some(matrix_sdk_base::event_cache::Gap { token: WINDOW_BOUNDARY.into() })),
        &events,
    );
    let mut updates = vec![Update::Clear];
    updates.extend(chunk.store_updates().take());
    store.handle_linked_chunk_updates(id, updates).await?;
    let (last, generator) = store.load_last_chunk(id).await?;
    let chunks: Vec<_> = chunk.chunks().collect();
    let metadata = chunks
        .iter()
        .enumerate()
        .map(|(index, c)| ChunkMetadata {
            num_items: match c.content() {
                ChunkContent::Items(events) => events.len(),
                ChunkContent::Gap(_) => 0,
            },
            previous: index.checked_sub(1).map(|i| chunks[i].identifier()),
            identifier: c.identifier(),
            next: chunks.get(index + 1).map(|n| n.identifier()),
        })
        .collect();
    Ok((last, generator, Some(metadata)))
}

/// Read only active stored windows for decryption retries. Never materialize a
/// legacy room-wide vector (which also contains detached and unopened threads).
#[cfg(feature = "e2e-encryption")]
pub(super) async fn stored_events(
    store: &EventCacheStoreLockGuard,
    ids: &[matrix_sdk_base::linked_chunk::OwnedLinkedChunkId],
    event_type: Option<&str>,
    session_id: Option<&str>,
) -> Result<Vec<Event>> {
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut bytes = 0;
    for id in ids {
        let mut scanned = 0;
        let id = id.as_ref();
        let (mut cursor, _) = store.load_last_chunk(id).await?;
        for _ in 0..MAX_CHUNKS {
            let Some(raw) = cursor else {
                break;
            };
            if scanned == MAX_EVENTS {
                break;
            }
            if let ChunkContent::Items(events) = raw.content {
                for event in events.into_iter().rev().take(MAX_EVENTS - scanned) {
                    scanned += 1;
                    if event_type.is_some_and(|ty| Some(ty) != event.kind.event_type().as_deref())
                        || session_id
                            .is_some_and(|session| Some(session) != event.kind.session_id())
                    {
                        continue;
                    }
                    let len = event.raw().json().get().len();
                    if len > MAX_EVENT_BYTES {
                        continue;
                    }
                    if event.event_id().is_some_and(|id| seen.contains(id)) {
                        continue;
                    }
                    if bytes + len > MAX_BYTES {
                        return Ok(result);
                    }
                    if let Some(id) = event.event_id() {
                        seen.insert(id.to_owned());
                    }
                    bytes += len;
                    result.push(event);
                }
            }
            if raw.previous.is_none() {
                break;
            }
            cursor = store.load_previous_chunk(id, raw.identifier).await?;
        }
    }
    Ok(result)
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests;

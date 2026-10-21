// Hagency local changes to matrix-sdk 0.19.1; Apache-2.0.
//! A disposable SDK working cache. Authoritative history stays on the server;
//! Hagency's independently encrypted search index stores searchable history.
//! Disk chunks are bounded too, so OrderTracker never loads N historical links.
use matrix_sdk_base::{
    event_cache::{Event, store::EventCacheStoreLockGuard},
    linked_chunk::{ChunkContent, LinkedChunkId, Update},
};

use super::{EventCacheError, Result, caches::event_linked_chunk::EventLinkedChunk};

pub(super) const MAX_EVENTS: usize = 512;
const KEEP_EVENTS: usize = 128;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 256 * 1024;

pub(super) fn validate<'a>(events: impl Iterator<Item = &'a Event>) -> Result<()> {
    let mut count = 0;
    let mut bytes = 0;
    for event in events {
        let len = event.raw().json().get().len();
        if len > MAX_EVENT_BYTES { return Err(EventCacheError::HistoryWindowFull); }
        count += 1;
        bytes += len;
        if count > MAX_EVENTS || bytes > MAX_BYTES { return Err(EventCacheError::HistoryWindowFull); }
    }
    Ok(())
}

/// Bound side-loaded event batches before collecting them in memory.
pub(super) fn collect(events:impl IntoIterator<Item=Event>) -> Result<Vec<Event>> {
    let mut result=Vec::new();
    let mut bytes=0usize;
    for event in events {
        validate(std::iter::once(&event))?;
        bytes+=event.raw().json().get().len();
        if result.len()>=MAX_EVENTS || bytes>MAX_BYTES {return Err(EventCacheError::HistoryWindowFull);}
        result.push(event);
    }
    Ok(result)
}

pub(super) fn compact(chunk: &mut EventLinkedChunk) -> bool {
    if validate(chunk.events().map(|(_, event)|event)).is_ok() { return false; }
    // A Clear plus append resets the SDK controller's positional/aggregation
    // metadata as well as its decoded items. Do not slice only the UI vector.
    let mut bytes = 0;
    let mut events: Vec<_> = chunk.revents().take(KEEP_EVENTS).take_while(|(_,event)| {
        let len = event.raw().json().get().len();
        bytes += len;
        len <= MAX_EVENT_BYTES && bytes <= MAX_BYTES
    }).map(|(_,event)|event.clone()).collect();
    events.reverse();
    chunk.reset();
    chunk.push_live_events(None, &events);
    true
}

/// Never load the old full metadata vector, including on first upgrade from a
/// previously unbounded SDK cache. The store lock serializes this with sync.
pub(super) async fn load(
    store: &EventCacheStoreLockGuard,
    id: LinkedChunkId<'_>,
) -> Result<EventLinkedChunk> {
    let last = match store.load_last_chunk(id).await {
        Ok((last, _)) => last,
        Err(error) => {
            // This is a disposable working cache. Match the upstream recovery
            // path for malformed/cyclic chunks without loading all metadata.
            tracing::error!(?error, "Resetting an unreadable bounded history cache");
            store.handle_linked_chunk_updates(id, vec![Update::Clear]).await?;
            None
        }
    };
    let had_chunk = last.is_some();
    let (gap, events) = match last.map(|c|c.content) {
        Some(ChunkContent::Items(events)) => (None, events),
        Some(ChunkContent::Gap(gap)) => (Some(gap), Vec::new()),
        None => (None, Vec::new()),
    };
    // A legacy last chunk can exceed the new byte budget. Keep a bounded
    // suffix rather than making the room impossible to open on upgrade.
    let mut bytes=0usize;
    let mut events:Vec<_>=events.into_iter().rev().filter(|event|event.raw().json().get().len()<=MAX_EVENT_BYTES)
        .take(KEEP_EVENTS).take_while(|event|{bytes+=event.raw().json().get().len();bytes<=MAX_BYTES}).collect();
    events.reverse();
    if let LinkedChunkId::Room(room)=id {
        // First upgrade also removes legacy, potentially unbounded detached
        // payloads and thread/pin chunks before any all-events redecryption.
        store.clear_all_events(Some(room)).await?;
    }
    let mut chunk = EventLinkedChunk::new();
    if gap.is_none() && events.is_empty() {
        if had_chunk && !matches!(id, LinkedChunkId::Room(_)) {
            store.handle_linked_chunk_updates(id, vec![Update::Clear]).await?;
        }
        // Preserve the new chunk's pending store updates for the first real
        // event. Creating an empty cache must not wait on a persistence write.
        return Ok(chunk);
    }
    chunk.push_live_events(gap, &events);
    let mut updates = vec![Update::Clear];
    updates.extend(chunk.store_updates().take());
    store.handle_linked_chunk_updates(id, updates).await?;
    let _ = chunk.updates_as_vector_diffs();
    Ok(chunk)
}

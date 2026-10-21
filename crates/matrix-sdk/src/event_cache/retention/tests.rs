// Hagency SDK history regressions; Apache-2.0.
use super::*;
#[cfg(feature = "e2e-encryption")]
use matrix_sdk_base::linked_chunk::OwnedLinkedChunkId;
use matrix_sdk_base::linked_chunk::{ChunkIdentifier, Position};
use matrix_sdk_test::{async_test, event_factory::EventFactory};
use ruma::{event_id, room_id, user_id};

async fn seed(
    store: &EventCacheStoreLockGuard,
    id: LinkedChunkId<'_>,
    chunks: usize,
    items: usize,
    body: &str,
) {
    let factory = EventFactory::new().room(id.room_id()).sender(user_id!("@budget:example.org"));
    let mut updates = vec![Update::Clear];
    for index in 0..chunks {
        let chunk = ChunkIdentifier::new(index as u64);
        updates.push(Update::NewItemsChunk {
            previous: index.checked_sub(1).map(|i| ChunkIdentifier::new(i as u64)),
            new: chunk,
            next: None,
        });
        let events = (0..items)
            .map(|item| {
                let event_id: ruma::OwnedEventId = format!(
                    "$budget-{}-{index}-{item}",
                    match id {
                        LinkedChunkId::Thread(_, root) => root.as_str(),
                        _ => "room",
                    }
                )
                .try_into()
                .unwrap();
                let message = factory.text_msg(body).event_id(&event_id);
                match id {
                    LinkedChunkId::Thread(_, root) => message.in_thread(root, root).into_event(),
                    _ => message.into_event(),
                }
            })
            .collect();
        updates.push(Update::PushItems { at: Position::new(chunk, 0), items: events });
    }
    store.handle_linked_chunk_updates(id, updates).await.unwrap();
}

#[async_test]
async fn test_small_window_preserves_ids_order_and_disk_lookup() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    seed(&store, id, 4, 2, "within budget").await;
    let (last, _, metadata) = load_snapshot(&store, id).await.unwrap();
    assert_eq!(last.unwrap().identifier, ChunkIdentifier::new(3));
    let metadata = metadata.unwrap();
    assert_eq!(metadata.iter().map(|m| m.num_items).sum::<usize>(), 8);
    assert_eq!(metadata.len(), 4);
    let chunk = load(&store, id).await.unwrap();
    assert_eq!(chunk.events().count(), 2, "payload loading remains lazy");
    assert_eq!(chunk.event_order(Position::new(ChunkIdentifier::new(0), 1)), Some(1));
    assert!(store.find_event(id.room_id(), event_id!("$budget-room-0-0")).await.unwrap().is_some());
}

#[async_test]
async fn test_room_load_and_trim_do_not_clear_sibling_thread_or_room() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let room = room_id!("!budget:example.org");
    let thread = LinkedChunkId::Thread(room, event_id!("$thread"));
    let other = LinkedChunkId::Room(room_id!("!other:example.org"));
    seed(&store, thread, 2, 1, "thread survives").await;
    seed(&store, other, 2, 1, "other room survives").await;
    for chunks in [2, MAX_EVENTS + 1] {
        seed(&store, LinkedChunkId::Room(room), chunks, 1, "room").await;
        load(&store, LinkedChunkId::Room(room)).await.unwrap();
        assert_eq!(store.load_all_chunks(thread).await.unwrap().len(), 2);
        assert_eq!(store.load_all_chunks(other).await.unwrap().len(), 2);
    }
}

#[async_test]
async fn test_exact_event_limit_is_kept_and_one_more_is_bounded() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    for id in [
        LinkedChunkId::Room(room_id!("!budget:example.org")),
        LinkedChunkId::Thread(room_id!("!budget:example.org"), event_id!("$thread")),
    ] {
        seed(&store, id, 4, 128, "limit").await;
        let (_, _, metadata) = load_snapshot(&store, id).await.unwrap();
        assert_eq!(metadata.unwrap().iter().map(|m| m.num_items).sum::<usize>(), MAX_EVENTS);
        seed(&store, id, MAX_EVENTS + 1, 1, "overflow").await;
        let mut chunk = load(&store, id).await.unwrap();
        assert!(chunk.events().count() <= KEEP_EVENTS);
        let (items, heads) = chunk.order_tracker.retained_metadata_size();
        assert!(items <= KEEP_EVENTS && heads <= 3);
        let disk = store.load_all_chunks(id).await.unwrap();
        assert!(disk.len() <= 3);
        assert!(
            disk.iter().any(
                |c| matches!(&c.content, ChunkContent::Gap(gap) if gap.token == WINDOW_BOUNDARY)
            )
        );
        let reopened = load(&store, id).await.unwrap();
        assert_eq!(reopened.events().count(), chunk.events().count());
    }
}

#[async_test]
async fn test_empty_chunk_metadata_is_bounded_too() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    seed(&store, id, MAX_CHUNKS, 0, "").await;
    assert_eq!(load_snapshot(&store, id).await.unwrap().2.unwrap().len(), MAX_CHUNKS);
    seed(&store, id, 10_000, 0, "").await;
    let (_, _, metadata) = load_snapshot(&store, id).await.unwrap();
    assert!(metadata.unwrap().len() <= 3);
    assert!(store.load_all_chunks(id).await.unwrap().len() <= 3);
}

#[async_test]
async fn test_bytes_and_single_event_limits_are_enforced_on_reopen() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    for (chunks, items, body) in [
        (2, 100, "x".repeat(64 * 1024)),
        (1, 1, "x".repeat(MAX_EVENT_BYTES)),
        (1, 300, "oversized legacy chunk".into()),
    ] {
        seed(&store, id, chunks, items, &body).await;
        let chunk = load(&store, id).await.unwrap();
        validate(chunk.events().map(|(_, e)| e)).unwrap();
        assert!(chunk.events().count() <= KEEP_EVENTS);
        assert!(store.load_all_chunks(id).await.unwrap().len() <= 3);
    }
}

#[async_test]
async fn test_compaction_bounds_unloaded_ordering_metadata() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    seed(&store, id, 4, 128, "old").await;
    let mut chunk = load(&store, id).await.unwrap();
    let new = EventFactory::new()
        .room(id.room_id())
        .sender(user_id!("@budget:example.org"))
        .text_msg("new")
        .event_id(event_id!("$new"))
        .into_event();
    chunk.push_events([new]);
    assert!(chunk.events().count() < MAX_EVENTS, "loaded vector alone would miss the disk prefix");
    assert!(compact(&mut chunk));
    let (items, heads) = chunk.order_tracker.retained_metadata_size();
    assert_eq!(items, KEEP_EVENTS);
    assert!(heads <= 3);
}

#[async_test]
async fn test_clear_leaves_no_empty_persisted_chunk_and_accepts_new_events() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    seed(&store, id, 1, 0, "").await;
    let mut chunk = load(&store, id).await.unwrap();
    assert!(store.load_all_chunks(id).await.unwrap().is_empty());
    let new = EventFactory::new()
        .room(id.room_id())
        .sender(user_id!("@budget:example.org"))
        .text_msg("after clear")
        .event_id(event_id!("$new"))
        .into_event();
    chunk.push_live_events(None, &[new]);
    store.handle_linked_chunk_updates(id, chunk.store_updates().take()).await.unwrap();
    assert_eq!(load(&store, id).await.unwrap().events().count(), 1);
}

#[cfg(feature = "e2e-encryption")]
#[async_test]
async fn test_stored_retry_scan_is_bounded_without_erasing_other_windows() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let room = room_id!("!budget:example.org");
    let thread = event_id!("$thread");
    let unopened = LinkedChunkId::Thread(room, event_id!("$unopened"));
    seed(&store, LinkedChunkId::Room(room), 10_000, 1, "legacy room").await;
    seed(&store, LinkedChunkId::Thread(room, thread), 2, 1, "active thread").await;
    seed(&store, unopened, 100, 1, "unopened").await;
    let events = stored_events(
        &store,
        &[
            OwnedLinkedChunkId::Room(room.to_owned()),
            OwnedLinkedChunkId::Thread(room.to_owned(), thread.to_owned()),
        ],
        Some("m.room.message"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(events.len(), MAX_EVENTS + 2);
    let all_types = stored_events(
        &store,
        &[OwnedLinkedChunkId::Thread(room.to_owned(), thread.to_owned())],
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(all_types.len(), 2, "None filters mean wildcard, including encryption-info refresh");
    assert!(events.iter().all(|e| e.event_id() != Some(event_id!("$budget-room-100-0"))));
    assert_eq!(store.load_all_chunks(unopened).await.unwrap().len(), 100);
}

#[async_test]
async fn test_oversized_event_is_a_boundary_not_a_hidden_hole() {
    let (client, _server) = crate::test_utils::client::isolated_local_client().await;
    let guard = client.event_cache_store().lock().await.unwrap();
    let store = guard.as_clean().unwrap();
    let id = LinkedChunkId::Room(room_id!("!budget:example.org"));
    let f = EventFactory::new().room(id.room_id()).sender(user_id!("@budget:example.org"));
    store
        .handle_linked_chunk_updates(
            id,
            vec![
                Update::NewItemsChunk { previous: None, new: ChunkIdentifier::new(0), next: None },
                Update::PushItems {
                    at: Position::new(ChunkIdentifier::new(0), 0),
                    items: vec![
                        f.text_msg("old").event_id(event_id!("$old")).into_event(),
                        f.text_msg("x".repeat(MAX_EVENT_BYTES))
                            .event_id(event_id!("$large"))
                            .into_event(),
                        f.text_msg("new").event_id(event_id!("$new")).into_event(),
                    ],
                },
            ],
        )
        .await
        .unwrap();
    let chunk = load(&store, id).await.unwrap();
    let ids: Vec<_> = chunk.events().map(|(_, e)| e.event_id().unwrap().to_owned()).collect();
    assert_eq!(
        ids,
        vec![event_id!("$new").to_owned()],
        "never bridge over a discarded middle event"
    );
}

#[cfg(feature = "sqlite")]
#[async_test]
async fn test_encrypted_sqlite_reopen_and_thread_isolation() {
    use matrix_sdk_base::store::StoreConfig;
    use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let room = room_id!("!budget:example.org");
    let thread = LinkedChunkId::Thread(room, event_id!("$thread"));
    for round in 0..2 {
        let sqlite = matrix_sdk_sqlite::SqliteEventCacheStore::open(
            directory.path().to_str().unwrap(),
            Some("isolated-budget-fixture"),
        )
        .await
        .unwrap();
        let client = crate::Client::builder()
            .homeserver_url("http://127.0.0.1")
            .store_config(
                StoreConfig::new(CrossProcessLockConfig::SingleProcess)
                    .event_cache_store(Arc::new(sqlite)),
            )
            .build()
            .await
            .unwrap();
        let guard = client.event_cache_store().lock().await.unwrap();
        let store = guard.as_clean().unwrap();
        if round == 0 {
            seed(store, thread, 2, 1, "thread persists").await;
            seed(store, LinkedChunkId::Room(room), 4, 128, "budget boundary").await;
            let (_, _, meta) = load_snapshot(store, LinkedChunkId::Room(room)).await.unwrap();
            assert_eq!(meta.unwrap().iter().map(|m| m.num_items).sum::<usize>(), MAX_EVENTS);
            seed(store, LinkedChunkId::Room(room), MAX_EVENTS + 1, 1, "trimmed suffix").await;
        }
        let chunk = load(store, LinkedChunkId::Room(room)).await.unwrap();
        assert_eq!(chunk.events().count(), 1);
        assert_eq!(
            chunk.events().next().unwrap().1.event_id(),
            Some(event_id!("$budget-room-512-0"))
        );
        assert!(store.load_all_chunks(LinkedChunkId::Room(room)).await.unwrap().len() <= 3);
        assert_eq!(store.load_all_chunks(thread).await.unwrap().len(), 2);
        assert!(
            store.load_all_chunks(LinkedChunkId::Room(room)).await.unwrap().iter().any(
                |c| matches!(&c.content, ChunkContent::Gap(gap) if gap.token == WINDOW_BOUNDARY)
            )
        );
    }
}

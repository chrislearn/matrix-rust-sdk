use std::io::Read;

use matrix_sdk_base::store::QueuedRequestKind;
use ruma::{event_id, mxc_uri, room_id, uint};
use wiremock::ResponseTemplate;

use super::*;
use crate::{
    attachment::{AttachmentConfig, Thumbnail},
    send_queue::RoomSendQueueUpdate,
    test_utils::mocks::MatrixMockServer,
};

async fn next_error(
    watch: &mut tokio::sync::broadcast::Receiver<RoomSendQueueUpdate>,
) -> Arc<crate::Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let RoomSendQueueUpdate::SendError { error, is_recoverable, .. } =
                watch.recv().await.unwrap()
            {
                assert!(is_recoverable);
                return error;
            }
        }
    })
    .await
    .unwrap()
}
async fn sent(
    watch: &mut tokio::sync::broadcast::Receiver<RoomSendQueueUpdate>,
) -> ruma::OwnedTransactionId {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match watch.recv().await.unwrap() {
                RoomSendQueueUpdate::SentEvent { transaction_id, .. } => return transaction_id,
                RoomSendQueueUpdate::SendError { error, is_recoverable, .. } => {
                    assert!(
                        is_recoverable
                            && matches!(
                                error.as_ref(),
                                crate::Error::UploadGovernor(UploadGovernorError::Capacity)
                            ),
                        "unexpected send failure: {error}"
                    );
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn restored_sqlite_main_thumbnail_progress_completion_keep_formats_and_transaction() {
    let mock = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    let governor = small();
    let client = mock
        .client_builder()
        .on_builder(|builder| {
            builder
                .sqlite_store(directory.path(), Some("fixture"))
                .upload_governor(governor.clone())
        })
        .build()
        .await;
    client.send_queue().set_enabled(false).await;
    let room = mock.sync_joined_room(&client, room_id!("!restored:test")).await;
    let queue = room.send_queue();
    let thumbnail = Thumbnail {
        data: vec![3; 8],
        content_type: mime::IMAGE_JPEG,
        height: uint!(2),
        width: uint!(4),
        size: uint!(8),
    };
    let original_txn = ruma::TransactionId::new();
    let handle = queue
        .send_attachment(
            "source",
            mime::IMAGE_JPEG,
            vec![7; 16],
            AttachmentConfig::new().thumbnail(Some(thumbnail)).txn_id(original_txn.clone()),
        )
        .await
        .unwrap();
    drop(handle);
    drop(queue);
    drop(room);
    let weak = client.downgrade();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.get().is_some() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    mock.mock_authenticated_media_config().ok_default().mount().await;
    mock.mock_room_state_encryption().plain().mount().await;
    mock.mock_upload()
        .respond_with(|request: &wiremock::Request| {
            let uri = if request.body == [3; 8] {
                "mxc://test/thumb"
            } else {
                assert_eq!(request.body, [7; 16]);
                "mxc://test/main"
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"content_uri":uri}))
        })
        .expect(2)
        .mount()
        .await;
    mock.mock_room_send().ok(event_id!("$restored")).mock_once().mount().await;
    let client = mock
        .client_builder()
        .on_builder(|builder| {
            builder
                .sqlite_store(directory.path(), Some("fixture"))
                .upload_governor(governor.clone())
        })
        .build()
        .await;
    client.send_queue().enable_upload_progress(true);
    let room = client.get_room(room_id!("!restored:test")).unwrap();
    let (_, mut watch) = room.send_queue().subscribe().await.unwrap();
    client.send_queue().set_enabled(true).await;
    assert_eq!(sent(&mut watch).await, original_txn);
    let store = client.media_store().lock().await.unwrap();
    let thumb = MediaSource::Plain(mxc_uri!("mxc://test/thumb").to_owned());
    assert_eq!(
        store
            .get_media_content_bounded(
                &MediaRequestParameters { source: thumb.clone(), format: MediaFormat::File },
                64
            )
            .await
            .unwrap()
            .unwrap(),
        [3; 8]
    );
    assert_eq!(
        store
            .get_media_content_bounded(
                &MediaRequestParameters {
                    source: thumb,
                    format: MediaFormat::Thumbnail(crate::media::MediaThumbnailSettings::new(
                        uint!(4),
                        uint!(2)
                    ))
                },
                64
            )
            .await
            .unwrap()
            .unwrap(),
        [3; 8]
    );
    assert!(
        client.state_store().load_send_queue_requests(room.room_id()).await.unwrap().is_empty()
    );
    state(&governor, |s| s.jobs == 0).await;
}
#[tokio::test]
async fn oversized_legacy_sqlite_blob_preserves_recoverable_row_instead_of_missing_or_wedge() {
    let mock = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    let governor = small();
    let client = mock
        .client_builder()
        .on_builder(|builder| {
            builder.sqlite_store(directory.path(), None).upload_governor(governor.clone())
        })
        .build()
        .await;
    client.send_queue().set_enabled(false).await;
    let room = mock.sync_joined_room(&client, room_id!("!oversize:test")).await;
    let queue = room.send_queue();
    queue
        .send_attachment("source", mime::IMAGE_JPEG, vec![7; 16], AttachmentConfig::new())
        .await
        .unwrap();
    let requests = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    let QueuedRequestKind::MediaUpload { cache_key, .. } = &requests[0].kind else {
        panic!("original media head")
    };
    client
        .media_store()
        .lock()
        .await
        .unwrap()
        .add_media_content(
            cache_key,
            vec![8; 65],
            matrix_sdk_base::media::store::IgnoreMediaRetentionPolicy::Yes,
        )
        .await
        .unwrap();
    let (_, mut watch) = queue.subscribe().await.unwrap();
    queue.set_enabled(true);
    let error = next_error(&mut watch).await;
    assert!(matches!(
        error.as_ref(),
        crate::Error::UploadGovernor(UploadGovernorError::TooLarge { max_bytes: 64 })
    ));
    let requests = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].error.is_none(), "resource policy must not persist a permanent wedge");
    assert!(!queue.is_enabled());
}
#[tokio::test]
async fn capacity_wait_retries_original_restored_transaction_without_another_ui_event() {
    let mock = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    // Seed durable outbox through the unchanged default SDK, then restore with
    // opt-in admission whose only slot is demonstrably occupied.
    let client = mock
        .client_builder()
        .on_builder(|builder| builder.sqlite_store(directory.path(), None))
        .build()
        .await;
    client.send_queue().set_enabled(false).await;
    let room = mock.sync_joined_room(&client, room_id!("!capacity:test")).await;
    let txn = ruma::TransactionId::new();
    let handle = room
        .send_queue()
        .send_attachment(
            "source",
            mime::IMAGE_JPEG,
            vec![7; 16],
            AttachmentConfig::new().txn_id(txn.clone()),
        )
        .await
        .unwrap();
    drop(handle);
    drop(room);
    let weak = client.downgrade();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.get().is_some() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let governor = Arc::new(
        UploadGovernor::new(UploadLimits {
            max_file_bytes: 64,
            max_jobs: 1,
            max_pending_write_bytes: 128,
        })
        .unwrap(),
    );
    let release = Arc::new(tokio::sync::Notify::new());
    let blocker = governor
        .submit::<(), UploadGovernorError, _, _>(0, {
            let release = release.clone();
            move |_, _| async move {
                release.notified().await;
                Ok(())
            }
        })
        .unwrap();
    state(&governor, |s| s.active == 1).await;
    let client = mock
        .client_builder()
        .on_builder(|builder| {
            builder.sqlite_store(directory.path(), None).upload_governor(governor.clone())
        })
        .build()
        .await;
    let room = client.get_room(room_id!("!capacity:test")).unwrap();
    let (_, mut watch) = room.send_queue().subscribe().await.unwrap();
    client.send_queue().set_enabled(true).await;
    assert!(matches!(
        next_error(&mut watch).await.as_ref(),
        crate::Error::UploadGovernor(UploadGovernorError::Capacity)
    ));
    assert!(room.send_queue().is_enabled());
    assert!(
        client.state_store().load_send_queue_requests(room.room_id()).await.unwrap()[0]
            .error
            .is_none()
    );
    mock.mock_authenticated_media_config().ok_default().mount().await;
    mock.mock_room_state_encryption().plain().mount().await;
    mock.mock_upload().ok(mxc_uri!("mxc://test/main")).mock_once().mount().await;
    mock.mock_room_send().ok(event_id!("$capacity")).mock_once().mount().await;
    release.notify_one();
    blocker.await.unwrap();
    assert_eq!(sent(&mut watch).await, txn);
}
#[cfg(feature = "e2e-encryption")]
#[tokio::test]
async fn encrypted_original_upload_uses_authenticated_descriptor_and_actual_ciphertext() {
    let mock = MatrixMockServer::new().await;
    let governor = small();
    let client = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    let room = mock.sync_joined_room(&client, room_id!("!encrypted:test")).await;
    mock.mock_room_state_encryption().encrypted().mount().await;
    mock.mock_authenticated_media_config().ok_default().mount().await;
    let received = Arc::new(Mutex::new(Vec::new()));
    mock.mock_upload()
        .expect_mime_type("application/octet-stream")
        .respond_with({
            let received = received.clone();
            move |request: &wiremock::Request| {
                *received.lock().unwrap() = request.body.clone();
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"content_uri":"mxc://test/encrypted"}))
            }
        })
        .mock_once()
        .mount()
        .await;
    client
        .media_store()
        .lock()
        .await
        .unwrap()
        .add_media_content(
            &request(),
            vec![7; 64],
            matrix_sdk_base::media::store::IgnoreMediaRetentionPolicy::Yes,
        )
        .await
        .unwrap();
    let source = governor
        .submit(0, move |custody, live| async move {
            upload_original(&room, request(), "image/jpeg".into(), None, 64, custody, live).await
        })
        .unwrap()
        .await
        .unwrap();
    let MediaSource::Encrypted(file) = source else {
        panic!("real encrypted room must produce encrypted media")
    };
    let cipher = received.lock().unwrap().clone();
    assert_ne!(cipher, [7; 64]);
    let mut cursor = std::io::Cursor::new(cipher);
    let mut decryptor =
        matrix_sdk_base::crypto::AttachmentDecryptor::new(&mut cursor, (*file).into()).unwrap();
    let mut plain = Vec::new();
    decryptor.read_to_end(&mut plain).unwrap();
    assert_eq!(plain, [7; 64]);
}
#[tokio::test]
async fn queued_original_checks_pause_before_read_and_http_without_losing_outbox() {
    let mock = MatrixMockServer::new().await;
    let governor = small();
    let client = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    client.send_queue().set_enabled(false).await;
    let room = mock.sync_joined_room(&client, room_id!("!pause:test")).await;
    let queue = room.send_queue();
    queue
        .send_attachment("source", mime::IMAGE_JPEG, vec![7; 16], AttachmentConfig::new())
        .await
        .unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let blocker = governor
        .submit::<(), UploadGovernorError, _, _>(0, {
            let release = release.clone();
            move |_, _| async move {
                release.notified().await;
                Ok(())
            }
        })
        .unwrap();
    state(&governor, |s| s.active == 1).await;
    let (_, mut watch) = queue.subscribe().await.unwrap();
    queue.set_enabled(true);
    state(&governor, |s| s.jobs == 2 && s.queued == 1).await;
    queue.set_enabled(false);
    release.notify_one();
    blocker.await.unwrap();
    assert!(matches!(
        next_error(&mut watch).await.as_ref(),
        crate::Error::UploadGovernor(UploadGovernorError::Paused)
    ));
    let rows = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].error.is_none());
}
#[tokio::test]
async fn stopped_completion_returns_original_store_guard_and_emits_visible_retained_error() {
    let mock = MatrixMockServer::new().await;
    let governor = small();
    let client = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    let room = mock.sync_joined_room(&client, room_id!("!stop:test")).await;
    mock.mock_authenticated_media_config().ok_default().mount().await;
    mock.mock_room_state_encryption().plain().mount().await;
    mock.mock_upload()
        .respond_with({
            let governor = governor.clone();
            move |_: &wiremock::Request| {
                governor.stop();
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"content_uri":"mxc://test/already-uploaded"}))
            }
        })
        .mock_once()
        .mount()
        .await;
    let queue = room.send_queue();
    let (_, mut watch) = queue.subscribe().await.unwrap();
    let handle = queue
        .send_attachment("source", mime::IMAGE_JPEG, vec![7; 16], AttachmentConfig::new())
        .await
        .unwrap();
    assert!(matches!(
        next_error(&mut watch).await.as_ref(),
        crate::Error::UploadGovernor(UploadGovernorError::Stopped)
    ));
    assert!(
        !client
            .state_store()
            .load_dependent_queued_requests(room.room_id())
            .await
            .unwrap()
            .is_empty(),
        "already-uploaded receipt retained without another HTTP send"
    );
    // A stopped admission returns its actual observer: no Store guard or newly
    // introduced capacity watcher may retain the old client indefinitely.
    drop(watch);
    drop(handle);
    drop(queue);
    drop(room);
    let weak = client.downgrade();
    drop(client);
    state(&governor, |s| s.jobs == 0).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.get().is_some() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("stopped original completion released store guard/client");
}

#[tokio::test]
async fn capacity_waiter_stops_without_holding_original_client_or_durable_store_guard() {
    let mock = MatrixMockServer::new().await;
    let governor = Arc::new(
        UploadGovernor::new(UploadLimits {
            max_file_bytes: 64,
            max_jobs: 1,
            max_pending_write_bytes: 128,
        })
        .unwrap(),
    );
    let client = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    client.send_queue().set_enabled(false).await;
    let room = mock.sync_joined_room(&client, room_id!("!stop-waiter:test")).await;
    let queue = room.send_queue();
    let handle = queue
        .send_attachment("source", mime::IMAGE_JPEG, vec![7; 16], AttachmentConfig::new())
        .await
        .unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let blocker = governor
        .submit::<(), UploadGovernorError, _, _>(0, {
            let release = release.clone();
            move |_, _| async move {
                release.notified().await;
                Ok(())
            }
        })
        .unwrap();
    state(&governor, |s| s.active == 1).await;
    let (_, mut watch) = queue.subscribe().await.unwrap();
    queue.set_enabled(true);
    assert!(matches!(
        next_error(&mut watch).await.as_ref(),
        crate::Error::UploadGovernor(UploadGovernorError::Capacity)
    ));
    governor.stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let error = next_error(&mut watch).await;
            match error.as_ref() {
                crate::Error::UploadGovernor(UploadGovernorError::Capacity) => {}
                crate::Error::UploadGovernor(UploadGovernorError::Stopped) => break,
                _ => panic!("unexpected original waiter status: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let rows = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].error.is_none());
    drop(watch);
    drop(handle);
    drop(queue);
    drop(room);
    let weak = client.downgrade();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.get().is_some() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("weak capacity metadata did not retain old Client");
    release.notify_one();
    blocker.await.unwrap();
    state(&governor, |s| s.jobs == 0).await;
}

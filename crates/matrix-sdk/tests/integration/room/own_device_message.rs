#![cfg(feature = "e2e-encryption")]
use matrix_sdk::{Client, test_utils::mocks::MatrixMockServer};
use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
use ruma::{RoomVersionId, TransactionId, device_id, event_id, room_id, user_id};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

async fn host(server: &MatrixMockServer) -> Client {
    let host = server
        .client_builder_for_crypto_end_to_end(user_id!("@owner:test"), device_id!("HOST"))
        .on_builder(|b| b.request_config(matrix_sdk::config::RequestConfig::new().retry_limit(3)))
        .build()
        .await;
    host.encryption().bootstrap_cross_signing(None).await.unwrap();
    server.mock_sync().ok_and_run(&host, |_| {}).await;
    host.encryption().request_user_identity(host.user_id().unwrap()).await.unwrap();
    assert!(
        host.encryption().get_own_device().await.unwrap().unwrap().is_verified_with_cross_signing()
    );
    host
}

#[async_test]
async fn test_own_device_seal_uses_detached_key_and_excludes_foreign_and_unsigned_devices() {
    let server = MatrixMockServer::new().await;
    server.mock_crypto_endpoints_preset().await;
    let host = host(&server).await;
    let remote = server.set_up_new_device_for_encryption(&host, device_id!("REMOTE"), vec![]).await;
    let unsigned =
        server.set_up_new_device_for_encryption(&host, device_id!("UNSIGNED"), vec![]).await;
    let foreign = server
        .client_builder_for_crypto_end_to_end(user_id!("@foreign:test"), device_id!("FOREIGN"))
        .build()
        .await;
    server.exchange_e2ee_identities(&host, &foreign).await;
    let owner = host.user_id().unwrap();
    host.encryption().request_user_identity(owner).await.unwrap();
    host.encryption()
        .get_device(owner, remote.device_id().unwrap())
        .await
        .unwrap()
        .unwrap()
        .verify()
        .await
        .unwrap();
    host.encryption().request_user_identity(owner).await.unwrap();
    assert!(
        host.encryption()
            .get_device(owner, remote.device_id().unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_verified_with_cross_signing()
    );
    assert!(
        !host
            .encryption()
            .get_device(owner, unsigned.device_id().unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_verified_with_cross_signing()
    );
    remote
        .encryption()
        .request_user_identity(owner)
        .await
        .unwrap()
        .unwrap()
        .verify()
        .await
        .unwrap();
    let room_id = room_id!("!private:test");
    let f = EventFactory::new().room(room_id).sender(owner);
    for client in [&host, &unsigned, &foreign] {
        server
            .mock_sync()
            .ok_and_run(client, |b| {
                b.add_joined_room(
                    JoinedRoomBuilder::new(room_id)
                        .add_state_event(f.create(owner, RoomVersionId::V10))
                        .add_state_event(f.room_encryption())
                        .add_state_event(f.member(owner))
                        .add_state_event(f.member(foreign.user_id().unwrap())),
                );
            })
            .await;
    }
    // Deliberately keep a broad ordinary room session, including a foreign
    // member. Sensitive sealing must not depend on this member cache or key.
    server
        .mock_get_members()
        .ok(vec![f.member(owner).into_raw(), f.member(foreign.user_id().unwrap()).into_raw()])
        .mount()
        .await;
    let queue = Arc::new(Mutex::new(Default::default()));
    let _capture = server.capture_put_to_device_traffic(owner, queue.clone()).await;
    let room = host.get_room(room_id).unwrap();
    let (old_rx, old_mock) = server.mock_room_send().ok_with_capture(event_id!("$old"), owner);
    old_mock.mock_once().mount().await;
    room.send_raw("m.room.message", json!({"msgtype":"m.text","body":"ordinary"})).await.unwrap();
    let old = old_rx.await.unwrap();
    for client in [&remote, &unsigned, &foreign] {
        server.sync_back_pending_to_device_messages(queue.clone(), client).await;
    }
    queue.lock().unwrap().clear();
    let sealed = room
        .seal_for_own_devices(
            "m.room.message",
            json!({"msgtype":"m.text","body":"private proposal","m.relates_to":{"rel_type":"m.thread","event_id":"$authenticated-inner","is_falling_back":false}}),
        )
        .await
        .unwrap();
    let (rx, mock) = server.mock_room_send().ok_with_capture(event_id!("$sensitive"), owner);
    mock.mock_once().mount().await;
    let result = sealed.send(&TransactionId::new(), || true).await.unwrap();
    assert!(result.encryption_info.is_some());
    let raw = rx.await.unwrap();
    let old_content: Value = old.get_field("content").unwrap().unwrap();
    let new_content: Value = raw.get_field("content").unwrap().unwrap();
    assert_ne!(old_content["session_id"], new_content["session_id"]);
    assert!(!raw.json().get().contains("private proposal"));
    {
        let keys = queue.lock().unwrap();
        assert_eq!(keys.len(), 1);
        let devices = keys.get(owner).unwrap();
        assert_eq!(devices.len(), 1);
        assert!(devices.contains_key(remote.device_id().unwrap()));
    }
    server.sync_back_pending_to_device_messages(queue.clone(), &remote).await;
    assert!(remote.get_room(room_id).is_none(), "receiver has no room history/cache");
    let before = server.server().received_requests().await.unwrap().len();
    let decrypted = remote
        .encryption()
        .decrypt_received_room_event(room_id, &ruma::serde::Raw::from_json(raw.json().to_owned()))
        .await
        .unwrap();
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);
    let clear: Value = serde_json::from_str(decrypted.raw().json().get()).unwrap();
    assert_eq!(clear["content"]["body"], "private proposal");
    assert_eq!(clear["content"]["m.relates_to"]["event_id"], "$authenticated-inner");
    let mut injected: Value = serde_json::from_str(raw.json().get()).unwrap();
    injected["content"]["m.relates_to"] =
        json!({"rel_type":"m.thread","event_id":"$forged-outer","is_falling_back":false});
    let authenticated = remote
        .encryption()
        .decrypt_received_room_event(
            room_id,
            &ruma::serde::Raw::from_json(serde_json::value::to_raw_value(&injected).unwrap()),
        )
        .await
        .unwrap();
    let inner: Value = serde_json::from_str(authenticated.raw().json().get()).unwrap();
    assert_eq!(inner["content"]["m.relates_to"]["event_id"], "$authenticated-inner");
    let mut fallback: Value = serde_json::from_str(old.json().get()).unwrap();
    fallback["content"]["m.relates_to"] =
        json!({"rel_type":"m.thread","event_id":"$forged-outer","is_falling_back":false});
    let no_fallback = remote
        .encryption()
        .decrypt_received_room_event(
            room_id,
            &ruma::serde::Raw::from_json(serde_json::value::to_raw_value(&fallback).unwrap()),
        )
        .await
        .unwrap();
    let no_fallback: Value = serde_json::from_str(no_fallback.raw().json().get()).unwrap();
    assert!(no_fallback["content"].get("m.relates_to").is_none());
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);

    for client in [&unsigned, &foreign] {
        let before = server.server().received_requests().await.unwrap().len();
        assert!(
            client
                .encryption()
                .decrypt_received_room_event(
                    room_id,
                    &ruma::serde::Raw::from_json(raw.json().to_owned())
                )
                .await
                .is_err(),
            "old broad session cannot decrypt the detached message"
        );
        assert_eq!(
            server.server().received_requests().await.unwrap().len(),
            before,
            "missing keys cannot start history/backup recovery"
        );
    }
    let (normal_rx, normal_mock) =
        server.mock_room_send().ok_with_capture(event_id!("$later"), owner);
    normal_mock.mock_once().mount().await;
    room.send_raw("m.room.message", json!({"msgtype":"m.text","body":"ordinary later"}))
        .await
        .unwrap();
    let normal = normal_rx.await.unwrap();
    let normal_content: Value = normal.get_field("content").unwrap().unwrap();
    assert_eq!(old_content["session_id"], normal_content["session_id"]);

    let stopped = room
        .seal_for_own_devices("m.room.message", json!({"body":"never published"}))
        .await
        .unwrap();
    let before = server.server().received_requests().await.unwrap().len();
    assert!(stopped.send(&TransactionId::new(), || false).await.is_err());
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);

    let stopped_after_keys = room
        .seal_for_own_devices("m.room.message", json!({"body":"never published"}))
        .await
        .unwrap();
    let before = server.server().received_requests().await.unwrap().len();
    let checks = std::sync::atomic::AtomicUsize::new(0);
    assert!(
        stopped_after_keys
            .send(&TransactionId::new(), || checks
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                == 0)
            .await
            .is_err()
    );
    let requests = server.server().received_requests().await.unwrap();
    let writes: Vec<_> = requests[before..].iter().filter(|r| r.method == "PUT").collect();
    assert_eq!(writes.len(), 1);
    assert!(writes[0].url.path().contains("/sendToDevice/"));

    // Even a client configured for retries must not repeat an unknown original
    // sensitive message write or let the caller recover a consumed capsule.
    let unknown = room
        .seal_for_own_devices("m.room.message", json!({"body":"uncertain publication"}))
        .await
        .unwrap();
    let before = server.server().received_requests().await.unwrap().len();
    let _unknown = wiremock::Mock::given(wiremock::matchers::method("PUT"))
        .and(wiremock::matchers::path_regex(r"/send/m.room.encrypted/"))
        .respond_with(
            wiremock::ResponseTemplate::new(500)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"original response lost"})),
        )
        .expect(1)
        .mount_as_scoped(server.server())
        .await;
    assert!(unknown.send(&TransactionId::new(), || true).await.is_err());
    let requests = server.server().received_requests().await.unwrap();
    assert_eq!(requests[before..].iter().filter(|r| r.url.path().contains("/send/")).count(), 1);
}

#[async_test]
async fn test_own_device_seal_refuses_without_cross_signed_remote_recipients() {
    let server = MatrixMockServer::new().await;
    server.mock_crypto_endpoints_preset().await;
    let host = host(&server).await;
    let _unsigned =
        server.set_up_new_device_for_encryption(&host, device_id!("UNSIGNED"), vec![]).await;
    let f = EventFactory::new().sender(host.user_id().unwrap());
    server
        .mock_sync()
        .ok_and_run(&host, |b| {
            b.add_joined_room(
                JoinedRoomBuilder::new(room_id!("!private:test"))
                    .add_state_event(f.room_encryption()),
            );
        })
        .await;
    let room = host.get_room(room_id!("!private:test")).unwrap();
    assert!(
        room.seal_for_own_devices("m.room.message", json!({"body":"private proposal"}))
            .await
            .is_err()
    );
    for request in server.server().received_requests().await.unwrap() {
        assert!(!request.url.path().contains("/send/"));
        assert!(!request.url.path().contains("/sendToDevice/"));
    }
}

use matrix_sdk::{Error, test_utils::mocks::MatrixMockServer};
use matrix_sdk_test::{DEFAULT_TEST_ROOM_ID, async_test};
use ruma::{event_id, events::room::message::RoomMessageEventContent};
use serde_json::json;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path_regex},
};

#[async_test]
async fn test_required_encryption_refuses_plain_room_before_any_send() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room = server.sync_joined_room(&client, &DEFAULT_TEST_ROOM_ID).await;
    server.mock_room_state_encryption().plain().mount().await;
    Mock::given(method("PUT"))
        .and(path_regex(r"/send/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id":"$leak"})))
        .expect(0)
        .mount(server.server())
        .await;

    assert!(matches!(
        room.send(RoomMessageEventContent::text_plain("secret")).require_encryption().await,
        Err(Error::EncryptionRequired)
    ));
    assert!(matches!(
        room.send_raw("m.room.message", json!({"body":"secret"})).require_encryption().await,
        Err(Error::EncryptionRequired)
    ));
}

#[cfg(feature = "e2e-encryption")]
#[async_test]
async fn test_required_encryption_encrypts_messages_and_reactions() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room = server.sync_joined_room(&client, &DEFAULT_TEST_ROOM_ID).await;
    server.mock_room_state_encryption().encrypted().mount().await;
    server.mock_get_members().ok(Vec::new()).mock_once().mount().await;
    Mock::given(method("PUT"))
        .and(path_regex(r"/send/m.room.encrypted/"))
        .respond_with(|request: &wiremock::Request| {
            let body: serde_json::Value = request.body_json().unwrap();
            assert_eq!(body["algorithm"], "m.megolm.v1.aes-sha2");
            assert!(body["ciphertext"].as_str().is_some_and(|s| !s.is_empty()));
            assert!(!String::from_utf8_lossy(&request.body).contains("private proposal"));
            ResponseTemplate::new(200).set_body_json(json!({"event_id":"$encrypted"}))
        })
        .expect(2)
        .mount(server.server())
        .await;
    let typed = room
        .send(RoomMessageEventContent::text_plain("private proposal"))
        .require_encryption()
        .await
        .unwrap();
    assert_eq!(typed.response.event_id, event_id!("$encrypted"));
    assert!(typed.encryption_info.is_some());
    let raw = room
        .send_raw("m.reaction", json!({"private proposal":true}))
        .require_encryption()
        .await
        .unwrap();
    assert!(raw.encryption_info.is_some());
}

#[async_test]
async fn test_optional_encryption_preserves_plain_room_sending() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room = server.sync_joined_room(&client, &DEFAULT_TEST_ROOM_ID).await;
    server.mock_room_state_encryption().plain().mount().await;
    server.mock_room_send().ok(event_id!("$plain")).mock_once().mount().await;
    let result = room.send(RoomMessageEventContent::text_plain("ordinary message")).await.unwrap();
    assert!(result.encryption_info.is_none());
}

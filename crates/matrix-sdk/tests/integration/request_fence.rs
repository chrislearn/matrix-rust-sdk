use matrix_sdk::{Client, HttpError, HttpRequestFence, config::RequestConfig};
use ruma::api::{MatrixVersion, client::directory::get_public_rooms};
use serde_json::json;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn client(
    server: &MockServer,
    fence: Arc<HttpRequestFence>,
    config: RequestConfig,
) -> Client {
    Client::builder()
        .homeserver_url(server.uri())
        .server_versions([MatrixVersion::V1_15])
        .request_fence(fence)
        .request_config(config)
        .build()
        .await
        .unwrap()
}
fn response() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({"chunk":[],"total_room_count_estimate":0}))
}
#[tokio::test]
async fn ordinary_client_without_fence_keeps_existing_http_behavior() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(response())
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::builder()
        .homeserver_url(server.uri())
        .server_versions([MatrixVersion::V1_15])
        .request_config(RequestConfig::new().disable_retry())
        .build()
        .await
        .unwrap();
    client.send(get_public_rooms::v3::Request::new()).await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
#[tokio::test]
async fn expired_host_fence_rejects_before_actual_http() {
    let server = MockServer::start().await;
    let live = Arc::new(AtomicBool::new(true));
    let proof = live.clone();
    let fence = Arc::new(HttpRequestFence::new(move || proof.load(Ordering::Acquire)));
    let client = client(&server, fence.clone(), RequestConfig::new().disable_retry()).await;
    live.store(false, Ordering::Release);
    assert!(matches!(
        client.send(get_public_rooms::v3::Request::new()).await,
        Err(HttpError::RequestFenceClosed)
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
    fence.retire_and_drain().await.unwrap();
}
#[tokio::test]
async fn retirement_after_real_http_attempt_prevents_retry() {
    let server = MockServer::start().await;
    let fence = Arc::new(HttpRequestFence::new(|| true));
    let stopped = fence.clone();
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(move |_: &wiremock::Request| {
            stopped.retire();
            ResponseTemplate::new(503)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"original transient failure"}))
        })
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server, fence.clone(), RequestConfig::new().retry_limit(3)).await;
    assert!(matches!(
        client.send(get_public_rooms::v3::Request::new()).await,
        Err(HttpError::RequestFenceClosed)
    ));
    fence.retire_and_drain().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
#[tokio::test]
async fn queued_request_rechecks_fence_and_drain_keeps_original_response() {
    let server = MockServer::start().await;
    let fence = Arc::new(HttpRequestFence::new(|| true));
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(response().set_delay(Duration::from_millis(150)))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(
        &server,
        fence.clone(),
        RequestConfig::new().disable_retry().max_concurrent_requests(NonZeroUsize::new(1)),
    )
    .await;
    let first = client.clone();
    let first = tokio::spawn(async move { first.send(get_public_rooms::v3::Request::new()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut queued = Box::pin(async { client.send(get_public_rooms::v3::Request::new()).await });
    assert!(futures_util::poll!(&mut queued).is_pending());
    let mut drain = Box::pin(fence.retire_and_drain());
    assert!(futures_util::poll!(&mut drain).is_pending());
    first.await.unwrap().unwrap();
    drain.await.unwrap();
    assert!(matches!(queued.await, Err(HttpError::RequestFenceClosed)));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn scoped_request_fence_cannot_bypass_client_fence_or_affect_ordinary_requests() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(response())
        .expect(1)
        .mount(&server)
        .await;
    let global = Arc::new(HttpRequestFence::new(|| true));
    let sdk = client(&server, global.clone(), RequestConfig::new().disable_retry()).await;
    let closed = Arc::new(HttpRequestFence::new(|| false));
    assert!(matches!(
        sdk.send(get_public_rooms::v3::Request::new()).with_request_fence(closed).await,
        Err(HttpError::RequestFenceClosed)
    ));
    sdk.send(get_public_rooms::v3::Request::new()).await.unwrap();
    global.retire();
    let live = Arc::new(HttpRequestFence::new(|| true));
    assert!(matches!(
        sdk.send(get_public_rooms::v3::Request::new()).with_request_fence(live).await,
        Err(HttpError::RequestFenceClosed)
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
#[tokio::test]
async fn scoped_request_fence_rechecks_real_queue_and_preserves_original_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(response().set_delay(Duration::from_millis(150)))
        .expect(1)
        .mount(&server)
        .await;
    let sdk = client(
        &server,
        Arc::new(HttpRequestFence::new(|| true)),
        RequestConfig::new().disable_retry().max_concurrent_requests(NonZeroUsize::new(1)),
    )
    .await;
    let first = Arc::new(HttpRequestFence::new(|| true));
    let original = sdk.clone();
    let scope = first.clone();
    let original = tokio::spawn(async move {
        original.send(get_public_rooms::v3::Request::new()).with_request_fence(scope).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let queued = Arc::new(HttpRequestFence::new(|| true));
    let mut next = Box::pin(async {
        sdk.send(get_public_rooms::v3::Request::new()).with_request_fence(queued.clone()).await
    });
    assert!(futures_util::poll!(&mut next).is_pending());
    queued.retire();
    let mut drain = Box::pin(first.retire_and_drain());
    assert!(futures_util::poll!(&mut drain).is_pending());
    original.await.unwrap().unwrap();
    drain.await.unwrap();
    assert!(matches!(next.await, Err(HttpError::RequestFenceClosed)));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
#[tokio::test]
async fn scoped_request_fence_retirement_after_actual_attempt_refuses_retry() {
    let server = MockServer::start().await;
    let scope = Arc::new(HttpRequestFence::new(|| true));
    let retire = scope.clone();
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/publicRooms"))
        .respond_with(move |_: &wiremock::Request| {
            retire.retire();
            ResponseTemplate::new(503)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"original failure"}))
        })
        .expect(1)
        .mount(&server)
        .await;
    let sdk = client(
        &server,
        Arc::new(HttpRequestFence::new(|| true)),
        RequestConfig::new().retry_limit(3),
    )
    .await;
    assert!(matches!(
        sdk.send(get_public_rooms::v3::Request::new()).with_request_fence(scope.clone()).await,
        Err(HttpError::RequestFenceClosed)
    ));
    scope.retire_and_drain().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn scoped_request_fence_unknown_token_cannot_start_automatic_refresh() {
    use matrix_sdk::{SessionMeta, SessionTokens, authentication::matrix::MatrixSession};
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/_matrix/client/v3/publicRooms")).respond_with(ResponseTemplate::new(401).set_body_json(json!({"errcode":"M_UNKNOWN_TOKEN","error":"original token refused","soft_logout":false}))).expect(1).mount(&server).await;
    let sdk = Client::builder()
        .homeserver_url(server.uri())
        .server_versions([MatrixVersion::V1_15])
        .request_config(RequestConfig::new().disable_retry())
        .handle_refresh_tokens()
        .build()
        .await
        .unwrap();
    sdk.matrix_auth()
        .restore_session(
            MatrixSession {
                meta: SessionMeta {
                    user_id: ruma::user_id!("@scope:test").to_owned(),
                    device_id: ruma::device_id!("SCOPED").to_owned(),
                },
                tokens: SessionTokens {
                    access_token: "test-access".into(),
                    refresh_token: Some("test-refresh".into()),
                },
            },
            Default::default(),
        )
        .await
        .unwrap();
    assert!(
        sdk.send(get_public_rooms::v3::Request::new())
            .with_request_fence(Arc::new(HttpRequestFence::new(|| true)))
            .await
            .is_err()
    );
    let received = server.received_requests().await.unwrap();
    assert!(received.iter().all(|r| !r.url.path().ends_with("/refresh")));
    assert_eq!(received.iter().filter(|r| r.url.path().ends_with("/publicRooms")).count(), 1);
}

#[cfg(feature = "e2e-encryption")]
#[tokio::test]
async fn scoped_identity_response_cannot_substitute_old_verified_cache_for_missing_or_invalid_keys()
{
    use matrix_sdk::test_utils::mocks::MatrixMockServer;
    use std::sync::atomic::AtomicUsize;
    let server = MatrixMockServer::new().await;
    server.mock_crypto_endpoints_preset().await;
    let sdk = server
        .client_builder_for_crypto_end_to_end(
            ruma::user_id!("@owner:test"),
            ruma::device_id!("HOST"),
        )
        .build()
        .await;
    sdk.encryption().bootstrap_cross_signing(None).await.unwrap();
    server.mock_sync().ok_and_run(&sdk, |_| {}).await;
    let user = sdk.user_id().unwrap();
    let device = sdk.device_id().unwrap();
    assert!(sdk.encryption().request_user_identity(user).await.unwrap().unwrap().is_verified());
    let valid: serde_json::Value = serde_json::from_slice(
        &sdk.http_client()
            .post(format!("{}/_matrix/client/v3/keys/query", server.uri()))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&json!({"device_keys":{user:[]}})).unwrap())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mode = Arc::new(AtomicUsize::new(0));
    let setting = mode.clone();
    let owner = user.to_string();
    let id = device.to_string();
    Mock::given(method("POST"))
        .and(path("/_matrix/client/v3/keys/query"))
        .respond_with(move |_: &wiremock::Request| {
            let mut body = valid.clone();
            match setting.load(Ordering::Acquire) {
                1 => {
                    body["failures"] =
                        json!({"test":{"errcode":"M_UNAVAILABLE","error":"partial query"}})
                }
                2 => {
                    body["master_keys"].as_object_mut().unwrap().remove(&owner);
                }
                3 => {
                    body["self_signing_keys"].as_object_mut().unwrap().remove(&owner);
                }
                4 => {
                    body["user_signing_keys"].as_object_mut().unwrap().remove(&owner);
                }
                5 => {
                    body["device_keys"][&owner].as_object_mut().unwrap().remove(&id);
                }
                6 => body["self_signing_keys"][&owner]["signatures"] = json!({}),
                7 => {
                    body["device_keys"][&owner][&id]["keys"][format!("ed25519:{id}")] =
                        json!("A".repeat(43))
                }
                _ => {}
            }
            ResponseTemplate::new(200).set_body_json(body)
        })
        .with_priority(1)
        .mount(server.server())
        .await;
    let encryption = sdk.encryption();
    let query = || {
        encryption.request_user_identity_with_fence(
            user,
            device,
            Arc::new(HttpRequestFence::new(|| true)),
        )
    };
    assert!(query().await.unwrap().unwrap().is_verified());
    for value in 1..=7 {
        mode.store(value, Ordering::Release);
        let result = query().await;
        if value == 2 {
            assert!(result.unwrap().is_none());
        } else {
            assert!(result.is_err(), "incomplete response mode {value} accepted");
        }
        // The old locally verified root still exists. Its latch is deliberately
        // insufficient to accept any of the above as a fresh trust observation.
        assert!(sdk.encryption().get_user_identity(user).await.unwrap().unwrap().is_verified());
    }
}

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

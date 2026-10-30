//! Production SDK metadata APIs against a streaming raw HTTP peer. The peer
//! generates large valid JSON from one 64KiB block. A bounded close probe lets
//! the old unbounded implementation receive the entire response and fail these
//! assertions rather than treating fixture truncation as a successful bound.
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::AtomicUsize,
};

use matrix_sdk_base::store::QueuedRequestKind;
use ruma::{
    api::{MatrixVersion, client::state::get_state_event_for_key},
    events::StateEventType,
};
use serde_json::json;

use super::*;
use crate::{
    ClientBuildError, HttpError, HttpRequestFence, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession, config::SyncSettings, send_queue::RoomSendQueueUpdate,
    test_utils::client::MockClientBuilder,
};

const TOTAL: usize = 8 * 1024 * 1024;
const BLOCK: usize = 64 * 1024;
#[derive(Clone, Copy, Debug)]
enum Target {
    Versions,
    Config,
    GzipConfig,
    Encryption,
    Topic,
    NonemptyEncryption,
}
impl Target {
    fn matches(self, path: &str) -> bool {
        match self {
            Self::Versions => path.ends_with("/_matrix/client/versions"),
            Self::Config | Self::GzipConfig => config_path(path),
            Self::Encryption => path.ends_with("/state/m.room.encryption/"),
            Self::Topic => path.ends_with("/state/m.room.topic/"),
            Self::NonemptyEncryption => path.ends_with("/state/m.room.encryption/nonempty"),
        }
    }
    fn limit(self) -> usize {
        if matches!(self, Self::Versions) { 256 * 1024 } else { BLOCK }
    }
    fn label(self) -> &'static str {
        match self {
            Self::Versions => "versions",
            Self::Config | Self::GzipConfig => "media_config",
            Self::Encryption => "encryption_state",
            _ => panic!("unlisted endpoint is not a preflight"),
        }
    }
    fn prefix(self, status: u16) -> &'static [u8] {
        if status != 200 {
            return if status == 404 {
                b"{\"errcode\":\"M_NOT_FOUND\",\"error\":\"fixture\",\"padding\":\""
            } else {
                b"{\"errcode\":\"M_UNKNOWN\",\"error\":\"fixture\",\"padding\":\""
            };
        }
        match self {
            Self::Versions => b"{\"versions\":[\"v1.15\"],\"unstable_features\":{},\"padding\":\"",
            Self::Config | Self::GzipConfig => b"{\"m.upload.size\":134217728,\"padding\":\"",
            Self::Encryption | Self::NonemptyEncryption => {
                b"{\"algorithm\":\"m.megolm.v1.aes-sha2\",\"padding\":\""
            }
            Self::Topic => b"{\"topic\":\"unchanged endpoint\",\"padding\":\"",
        }
    }
}
fn config_path(path: &str) -> bool {
    path.ends_with("/_matrix/client/v1/media/config")
        || path.ends_with("/_matrix/media/v3/config")
        || path.ends_with("/_matrix/media/r0/config")
}
#[derive(Clone, Copy, Debug)]
enum Framing {
    Declared,
    Chunked,
    FalseLength,
}
#[derive(Clone, Copy)]
struct Scenario {
    target: Target,
    framing: Framing,
    total: usize,
    status: u16,
    malformed: bool,
    refresh: bool,
}
impl Scenario {
    fn large(target: Target, framing: Framing) -> Self {
        Self { target, framing, total: TOTAL, status: 200, malformed: false, refresh: false }
    }
}
#[derive(Default)]
struct Counts {
    sent: AtomicUsize,
    target: AtomicUsize,
    posts: AtomicUsize,
    refreshes: AtomicUsize,
    requests: Mutex<Vec<(String, String)>>,
}
struct Peer {
    url: String,
    stop: Arc<AtomicBool>,
    counts: Arc<Counts>,
    join: Option<JoinHandle<()>>,
}
impl Peer {
    fn start(scenario: Scenario) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let counts = Arc::new(Counts::default());
        let join = std::thread::spawn({
            let stop = stop.clone();
            let counts = counts.clone();
            move || {
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((socket, _)) => {
                            // macOS accepts may inherit nonblocking mode.
                            socket.set_nonblocking(false).unwrap();
                            socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                            socket.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
                            if let Err(error) = serve(socket, scenario, &counts, &stop)
                                && !matches!(
                                    error.kind(),
                                    std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionAborted
                                )
                            {
                                panic!("metadata fixture original IO: {error}");
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("metadata fixture accept: {error}"),
                    }
                }
            }
        });
        Self { url, stop, counts, join: Some(join) }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.join.take().unwrap().join().unwrap();
    }
}
fn small(socket: &mut TcpStream, status: u16, body: &[u8]) -> std::io::Result<()> {
    write!(
        socket,
        "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    socket.write_all(body)?;
    socket.flush()
}
fn serve(
    socket: TcpStream,
    scenario: Scenario,
    counts: &Counts,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(());
    }
    let path = line.split_whitespace().nth(1).unwrap().split('?').next().unwrap().to_owned();
    let mut headers = line.len();
    let mut length = 0usize;
    let mut authorization = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        headers += line.len();
        if headers > 16 * 1024 {
            return Err(std::io::Error::other("fixture header bound"));
        }
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
            if name.eq_ignore_ascii_case("authorization") {
                authorization = value.trim().to_owned();
            }
        }
    }
    // Consume actual POST bodies before replying; closing with unread body can
    // cause TCP reset and conceal a valid refresh response on some platforms.
    let mut block = [0u8; BLOCK];
    while length > 0 {
        let bound = length.min(block.len());
        let count = reader.read(&mut block[..bound])?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        length -= count;
    }
    counts.requests.lock().unwrap().push((path.clone(), authorization.clone()));
    let socket = reader.get_mut();
    if path.ends_with("/_matrix/client/v3/refresh") {
        counts.refreshes.fetch_add(1, Ordering::AcqRel);
        return small(socket, 200, br#"{"access_token":"NEW_ACCESS","refresh_token":"NEW_REFRESH","expires_in_ms":3600000}"#);
    }
    if path.ends_with("/_matrix/media/v3/upload")
        || path.ends_with("/_matrix/client/v1/media/upload")
    {
        counts.posts.fetch_add(1, Ordering::AcqRel);
        return small(socket, 200, br#"{"content_uri":"mxc://test/accepted"}"#);
    }
    if scenario.target.matches(&path) {
        let attempt = counts.target.fetch_add(1, Ordering::AcqRel);
        if scenario.refresh {
            if attempt == 0 {
                assert_eq!(authorization, "Bearer ORIGINAL_ACCESS");
                return small(socket, 401, br#"{"errcode":"M_UNKNOWN_TOKEN","error":"fixture expired","soft_logout":false}"#);
            }
            assert_eq!(authorization, "Bearer NEW_ACCESS");
            return small(socket, 200, br#"{"m.upload.size":134217728}"#);
        }
        if scenario.malformed {
            return small(socket, 200, b"not-json");
        }
        if matches!(scenario.target, Target::GzipConfig) {
            // Deterministic valid JSON expands to exactly 64KiB + one byte.
            // The complete compressed representation is far below the budget.
            const BODY: &[u8] = &[
                31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 237, 193, 91, 10, 64, 80, 20, 0, 192, 189, 156,
                111, 41, 143, 34, 187, 185, 117, 37, 229, 85, 242, 67, 246, 110, 31, 154, 153, 39,
                214, 242, 58, 150, 61, 229, 242, 156, 239, 49, 134, 170, 105, 235, 170, 235, 234,
                190, 136, 35, 229, 60, 111, 83, 12, 145, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 248, 183, 120, 63,
                58, 149, 252, 25, 1, 0, 1, 0,
            ];
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                BODY.len()
            )?;
            socket.write_all(BODY)?;
            counts.sent.fetch_add(BODY.len(), Ordering::AcqRel);
            return socket.flush();
        }
        match scenario.framing {
            Framing::Declared => write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                scenario.status, scenario.total
            )?,
            Framing::Chunked => write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                scenario.status
            )?,
            Framing::FalseLength => write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                scenario.status
            )?,
        }
        let prefix = scenario.target.prefix(scenario.status);
        let suffix = b"\"}";
        assert!(scenario.total >= prefix.len() + suffix.len());
        let chunked = !matches!(scenario.framing, Framing::Declared);
        let mut position = 0;
        while position < scenario.total && !stop.load(Ordering::Acquire) {
            let count = BLOCK.min(scenario.total - position);
            block[..count].fill(b'a');
            if position == 0 {
                block[..prefix.len()].copy_from_slice(prefix);
            }
            let suffix_start = scenario.total - suffix.len();
            if suffix_start < position + count {
                let start = suffix_start.saturating_sub(position);
                let offset = position + start - suffix_start;
                block[start..count].copy_from_slice(&suffix[offset..offset + count - start]);
            }
            if chunked {
                write!(socket, "{count:x}\r\n")?;
            }
            socket.write_all(&block[..count])?;
            if chunked {
                socket.write_all(b"\r\n")?;
            }
            socket.flush()?;
            position += count;
            counts.sent.fetch_add(count, Ordering::AcqRel);
            if position == scenario.target.limit() + BLOCK && scenario.total > position {
                let mut probe = [0; 1];
                match socket.read(&mut probe) {
                    Ok(0) => return Ok(()),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        return Ok(());
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    result => {
                        result?;
                    }
                }
            }
        }
        if chunked && position == scenario.total {
            socket.write_all(b"0\r\n\r\n")?;
        }
        return socket.flush();
    }
    let (status, body) = if path.contains("/_matrix/client/") && path.ends_with("/sync") {
        (
            200,
            json!({"next_batch":"metadata-fixture","rooms":{"join":{"!metadata:test":{"state":{"events":[{"type":"m.room.create","state_key":"","event_id":"$create","sender":"@example:localhost","origin_server_ts":1,"content":{"creator":"@example:localhost","room_version":"10"}},{"type":"m.room.member","state_key":"@example:localhost","event_id":"$join","sender":"@example:localhost","origin_server_ts":2,"content":{"membership":"join"}}]},"timeline":{"events":[],"limited":false,"prev_batch":"empty"},"ephemeral":{"events":[]},"account_data":{"events":[]},"unread_notifications":{}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}}),
        )
    } else if path.ends_with("/_matrix/client/versions") {
        (200, json!({"versions":["v1.15"],"unstable_features":{}}))
    } else if config_path(&path) {
        (200, json!({"m.upload.size":128*1024*1024}))
    } else {
        (404, json!({"errcode":"M_NOT_FOUND","error":"fixture missing"}))
    };
    small(socket, status, &serde_json::to_vec(&body).unwrap())
}
fn governor() -> Arc<UploadGovernor> {
    Arc::new(UploadGovernor::new(UploadLimits::default()).unwrap())
}
async fn make_client(peer: &Peer, governed: bool, version: MatrixVersion) -> Client {
    MockClientBuilder::new(Some(&peer.url))
        .server_versions(vec![version])
        .on_builder(|builder| if governed { builder.upload_governor(governor()) } else { builder })
        .build()
        .await
}
fn assert_metadata(error: &HttpError, target: Target) {
    if let HttpError::Cached(inner) = error {
        return assert_metadata(inner, target);
    }
    assert!(
        matches!(error, HttpError::MetadataResponseTooLarge { endpoint, max_bytes }
        if *endpoint == target.label() && *max_bytes == target.limit()),
        "actual error: {error:?}"
    );
    assert!(matches!(error.retry_kind(), crate::error::RetryKind::Permanent));
}
fn http(error: &crate::Error) -> &HttpError {
    let crate::Error::Http(error) = error else {
        panic!("expected actual metadata HTTP error: {error:?}")
    };
    error
}
async fn oversized_versions(framing: Framing) {
    let peer = Peer::start(Scenario::large(Target::Versions, framing));
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    let error = client.fetch_server_versions(None).await.unwrap_err();
    assert_metadata(&error, Target::Versions);
    assert_eq!(peer.counts.target.load(Ordering::Acquire), 1);
    assert!(peer.counts.sent.load(Ordering::Acquire) < TOTAL);
}
#[tokio::test]
async fn versions_declared_length_stops_before_full_body() {
    oversized_versions(Framing::Declared).await;
}
#[tokio::test]
async fn versions_chunked_body_has_actual_cumulative_limit() {
    oversized_versions(Framing::Chunked).await;
}
#[tokio::test]
async fn versions_false_small_length_cannot_bypass_chunked_limit() {
    oversized_versions(Framing::FalseLength).await;
}
#[tokio::test]
async fn builder_discovery_uses_policy_before_client_exists() {
    let peer = Peer::start(Scenario::large(Target::Versions, Framing::Chunked));
    let policy = governor();
    let error = Client::builder()
        .homeserver_url(&peer.url)
        .upload_governor(policy.clone())
        .sliding_sync_version_builder(crate::sliding_sync::VersionBuilder::DiscoverNative)
        .build()
        .await
        .unwrap_err();
    let ClientBuildError::Http(error) = error else { panic!("original builder error: {error:?}") };
    assert_metadata(&error, Target::Versions);
    assert_eq!(policy.stats().jobs, 0, "pre-auth discovery starts no transfer worker");
}
#[tokio::test]
async fn cold_upload_size_versions_path_is_bounded_without_inventing_post_unknown() {
    let peer = Peer::start(Scenario::large(Target::Versions, Framing::FalseLength));
    let client = MockClientBuilder::new(Some(&peer.url))
        .no_server_versions()
        .on_builder(|builder| builder.upload_governor(governor()))
        .build()
        .await;
    let error = client.load_or_fetch_max_upload_size().await.unwrap_err();
    assert_metadata(http(&error), Target::Versions);
    assert!(!is_unknown_upload(&error));
    assert_eq!(peer.counts.posts.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn authenticated_and_legacy_media_config_paths_both_have_real_limits() {
    for version in [MatrixVersion::V1_15, MatrixVersion::V1_10] {
        for framing in [Framing::Declared, Framing::Chunked, Framing::FalseLength] {
            let peer = Peer::start(Scenario::large(Target::Config, framing));
            let client = make_client(&peer, true, version).await;
            let error = client.load_or_fetch_max_upload_size().await.unwrap_err();
            assert_metadata(http(&error), Target::Config);
            let requests = peer.counts.requests.lock().unwrap();
            assert!(requests.iter().any(|(path, _)| if version == MatrixVersion::V1_15 {
                path.ends_with("/_matrix/client/v1/media/config")
            } else {
                path.ends_with("/_matrix/media/v3/config")
            }));
            assert_eq!(peer.counts.target.load(Ordering::Acquire), 1);
            assert!(peer.counts.sent.load(Ordering::Acquire) < TOTAL);
            assert_eq!(peer.counts.posts.load(Ordering::Acquire), 0);
        }
    }
}
#[tokio::test]
async fn encryption_error_status_is_bounded_and_never_cached_as_plain() {
    let mut scenario = Scenario::large(Target::Encryption, Framing::FalseLength);
    scenario.status = 404;
    let peer = Peer::start(scenario);
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    client.sync_once(SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
    let room = client.get_room(ruma::room_id!("!metadata:test")).unwrap();
    assert!(room.encryption_state().is_unknown());
    let error = room.latest_encryption_state().await.unwrap_err();
    assert_metadata(http(&error), Target::Encryption);
    assert!(room.encryption_state().is_unknown(), "oversized 404 is not trusted NotFound evidence");
    assert_eq!(peer.counts.posts.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn accepted_metadata_at_each_exact_limit_keeps_normal_parse_semantics() {
    for target in [Target::Versions, Target::Config, Target::Encryption] {
        let mut scenario = Scenario::large(target, Framing::Chunked);
        scenario.total = target.limit();
        let peer = Peer::start(scenario);
        let client = make_client(&peer, true, MatrixVersion::V1_15).await;
        match target {
            Target::Versions => {
                client.fetch_server_versions(None).await.unwrap();
            }
            Target::Config => {
                assert_eq!(
                    u64::from(client.load_or_fetch_max_upload_size().await.unwrap()),
                    128 * 1024 * 1024
                );
            }
            Target::Encryption => {
                client.sync_once(SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
                let room = client.get_room(ruma::room_id!("!metadata:test")).unwrap();
                assert!(room.latest_encryption_state().await.unwrap().is_encrypted());
            }
            _ => unreachable!(),
        }
        assert_eq!(peer.counts.sent.load(Ordering::Acquire), target.limit());
    }
}
#[tokio::test]
async fn oversized_503_is_permanent_and_does_not_enter_server_retry_loop() {
    let mut scenario = Scenario::large(Target::Config, Framing::Chunked);
    scenario.status = 503;
    let peer = Peer::start(scenario);
    let client = MockClientBuilder::new(Some(&peer.url))
        .on_builder(|builder| {
            builder.upload_governor(governor()).request_config(RequestConfig::short_retry())
        })
        .build()
        .await;
    assert_metadata(
        http(&client.load_or_fetch_max_upload_size().await.unwrap_err()),
        Target::Config,
    );
    assert_eq!(peer.counts.target.load(Ordering::Acquire), 1);
}
#[tokio::test]
async fn ordinary_large_default_and_unrelated_state_responses_are_unchanged() {
    let peer = Peer::start(Scenario::large(Target::Versions, Framing::Declared));
    let client = make_client(&peer, false, MatrixVersion::V1_15).await;
    client.fetch_server_versions(None).await.unwrap();
    assert_eq!(peer.counts.sent.load(Ordering::Acquire), TOTAL);
    for (target, event_type, state_key) in [
        (Target::Topic, StateEventType::RoomTopic, ""),
        (Target::NonemptyEncryption, StateEventType::RoomEncryption, "nonempty"),
    ] {
        let peer = Peer::start(Scenario::large(target, Framing::Chunked));
        let client = make_client(&peer, true, MatrixVersion::V1_15).await;
        client
            .send(get_state_event_for_key::v3::Request::new(
                ruma::room_id!("!encoded/room:test").into(),
                event_type,
                state_key.into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            peer.counts.sent.load(Ordering::Acquire),
            TOTAL,
            "unlisted state endpoint remains unchanged"
        );
    }
}
#[tokio::test]
async fn proxy_base_path_and_percent_encoded_room_keep_exact_encryption_selection() {
    let peer = Peer::start(Scenario::large(Target::Encryption, Framing::Chunked));
    let client = MockClientBuilder::new(Some(&format!("{}/proxy-base", peer.url)))
        .on_builder(|builder| builder.upload_governor(governor()))
        .build()
        .await;
    let error = client
        .send(get_state_event_for_key::v3::Request::new(
            ruma::room_id!("!encoded/room:test").into(),
            StateEventType::RoomEncryption,
            "".into(),
        ))
        .await
        .unwrap_err();
    assert_metadata(&error, Target::Encryption);
    assert!(
        peer.counts
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path.starts_with("/proxy-base/") && path.contains("%2F"))
    );
}
#[tokio::test]
async fn accepted_401_config_refresh_keeps_original_auth_and_client_fence() {
    let mut scenario = Scenario::large(Target::Config, Framing::Declared);
    scenario.refresh = true;
    let peer = Peer::start(scenario);
    let fence = Arc::new(HttpRequestFence::new(|| true));
    let client = MockClientBuilder::new(Some(&peer.url))
        .unlogged()
        .on_builder(|builder| {
            builder.upload_governor(governor()).handle_refresh_tokens().request_fence(fence.clone())
        })
        .build()
        .await;
    client
        .matrix_auth()
        .restore_session(
            MatrixSession {
                meta: SessionMeta {
                    user_id: ruma::user_id!("@example:localhost").into(),
                    device_id: ruma::device_id!("SOURCE").into(),
                },
                tokens: SessionTokens {
                    access_token: "ORIGINAL_ACCESS".into(),
                    refresh_token: Some("ORIGINAL_REFRESH".into()),
                },
            },
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(u64::from(client.load_or_fetch_max_upload_size().await.unwrap()), 128 * 1024 * 1024);
    assert_eq!(client.access_token().as_deref(), Some("NEW_ACCESS"));
    assert_eq!(peer.counts.refreshes.load(Ordering::Acquire), 1);
    assert_eq!(peer.counts.target.load(Ordering::Acquire), 2);
    fence.retire();
    let error = client.fetch_server_versions(None).await.unwrap_err();
    assert!(matches!(error, HttpError::RequestFenceClosed));
    assert!(
        !peer.counts.requests.lock().unwrap().iter().any(|(path, _)| path.ends_with("/versions")),
        "closed original fence prevents the next metadata HTTP request"
    );
}
#[tokio::test]
async fn accepted_malformed_and_missing_auth_keep_original_error_categories() {
    let mut scenario = Scenario::large(Target::Config, Framing::Declared);
    scenario.malformed = true;
    let peer = Peer::start(scenario);
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    assert!(matches!(
        http(&client.load_or_fetch_max_upload_size().await.unwrap_err()),
        HttpError::Api(_)
    ));
    let no_auth = MockClientBuilder::new(Some(&peer.url))
        .unlogged()
        .on_builder(|builder| builder.upload_governor(governor()))
        .build()
        .await;
    assert!(matches!(
        http(&no_auth.load_or_fetch_max_upload_size().await.unwrap_err()),
        HttpError::IntoHttp(_)
    ));
    assert_eq!(
        peer.counts.target.load(Ordering::Acquire),
        1,
        "serialization failure creates no second HTTP request"
    );
}
#[tokio::test]
async fn actual_governed_outbox_keeps_original_row_and_never_posts_after_config_rejection() {
    let peer = Peer::start(Scenario::large(Target::Config, Framing::FalseLength));
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    client.sync_once(SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
    let room = client.get_room(ruma::room_id!("!metadata:test")).unwrap();
    let queue = room.send_queue();
    let (_, mut updates) = queue.subscribe().await.unwrap();
    queue
        .send_attachment(
            "source",
            mime::IMAGE_JPEG,
            vec![7; 16],
            crate::attachment::AttachmentConfig::new(),
        )
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let RoomSendQueueUpdate::SendError { error, is_recoverable, .. } =
                updates.recv().await.unwrap()
            {
                assert!(!is_recoverable);
                break error;
            }
        }
    })
    .await
    .unwrap();
    assert_metadata(http(&error), Target::Config);
    assert!(!is_unknown_upload(&error), "preflight is not an admitted POST outcome");
    let rows = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    assert_eq!(rows.len(), 1);
    let QueuedRequestKind::MediaUpload { cache_key, .. } = &rows[0].kind else {
        panic!("original media transaction must remain")
    };
    assert!(rows[0].error.is_some());
    assert_eq!(
        client
            .media_store()
            .lock()
            .await
            .unwrap()
            .get_media_content_bounded(cache_key, 16)
            .await
            .unwrap()
            .unwrap(),
        vec![7; 16]
    );
    assert_eq!(peer.counts.posts.load(Ordering::Acquire), 0);
    let policy = client.send_queue().upload_governor().unwrap();
    state(&policy, |stats| stats.jobs == 0).await;
}

#[tokio::test]
async fn one_decoded_byte_over_each_limit_is_rejected_without_truncation() {
    for target in [Target::Versions, Target::Config, Target::Encryption] {
        let mut scenario = Scenario::large(target, Framing::Chunked);
        scenario.total = target.limit() + 1;
        let peer = Peer::start(scenario);
        let client = make_client(&peer, true, MatrixVersion::V1_15).await;
        match target {
            Target::Versions => {
                assert_metadata(&client.fetch_server_versions(None).await.unwrap_err(), target)
            }
            Target::Config => assert_metadata(
                http(&client.load_or_fetch_max_upload_size().await.unwrap_err()),
                target,
            ),
            Target::Encryption => {
                client.sync_once(SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
                let room = client.get_room(ruma::room_id!("!metadata:test")).unwrap();
                assert_metadata(http(&room.latest_encryption_state().await.unwrap_err()), target);
                assert!(room.encryption_state().is_unknown());
            }
            _ => unreachable!(),
        }
    }
}
#[tokio::test]
async fn accepted_notfound_keeps_existing_plain_room_semantics() {
    let mut scenario = Scenario::large(Target::Encryption, Framing::Chunked);
    scenario.status = 404;
    scenario.total = BLOCK;
    let peer = Peer::start(scenario);
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    client.sync_once(SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
    let room = client.get_room(ruma::room_id!("!metadata:test")).unwrap();
    assert!(room.encryption_state().is_unknown());
    room.request_encryption_state().await.unwrap();
    assert!(!room.encryption_state().is_unknown());
    assert!(!room.encryption_state().is_encrypted());
}

#[tokio::test]
async fn gzip_wire_length_cannot_bypass_decoded_config_budget() {
    let peer = Peer::start(Scenario::large(Target::GzipConfig, Framing::Declared));
    let client = make_client(&peer, true, MatrixVersion::V1_15).await;
    assert_metadata(
        http(&client.load_or_fetch_max_upload_size().await.unwrap_err()),
        Target::Config,
    );
    assert!(
        peer.counts.sent.load(Ordering::Acquire) < BLOCK,
        "actual compressed wire bytes fit while decoded response exceeds the budget"
    );
}

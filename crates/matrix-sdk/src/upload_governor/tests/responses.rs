//! Actual raw peer responses. The fixture holds only a 64KiB generator buffer;
//! after two chunks it observes actual peer closure, with a bounded fallback
//! that sends the rest so the old unbounded collector cannot pass by
//! truncation.
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::AtomicUsize,
};

use matrix_sdk_base::store::QueueWedgeError;
use serde_json::json;

use super::*;
use crate::{
    attachment::AttachmentConfig, send_queue::RoomSendQueueUpdate,
    test_utils::client::MockClientBuilder,
};
const TOTAL: usize = 8 * 1024 * 1024;
#[derive(Clone, Copy)]
enum Framing {
    Declared,
    Chunked,
    LyingChunked,
}
struct Peer {
    url: String,
    stop: Arc<AtomicBool>,
    sent: Arc<AtomicUsize>,
    join: Option<JoinHandle<()>>,
}
impl Peer {
    fn start(framing: Framing) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicUsize::new(0));
        let join = std::thread::spawn({
            let stop = stop.clone();
            let sent = sent.clone();
            move || {
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((socket, _)) => {
                            socket.set_nonblocking(false).unwrap();
                            socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                            socket.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
                            if let Err(error) = serve(socket, framing, &sent, &stop)
                                && !matches!(
                                    error.kind(),
                                    std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::UnexpectedEof
                                )
                            {
                                panic!("actual response fixture IO failed: {error}");
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(error) => panic!("actual response fixture accept: {error}"),
                    }
                }
            }
        });
        Self { url, stop, sent, join: Some(join) }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.join.take().unwrap().join().unwrap();
    }
}
fn serve(
    socket: TcpStream,
    framing: Framing,
    sent: &AtomicUsize,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.is_empty() {
        return Ok(());
    }
    let path = line.split_whitespace().nth(1).unwrap().to_owned();
    let mut headers = line.len();
    let mut length = 0usize;
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
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap();
        }
    }
    let mut bytes = [0; 64 * 1024];
    while length > 0 {
        let bound = length.min(bytes.len());
        let n = reader.read(&mut bytes[..bound])?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        length -= n;
    }
    let socket = reader.get_mut();
    if path.starts_with("/_matrix/media/v3/upload") {
        match framing {
            Framing::Declared => write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {TOTAL}\r\nConnection: close\r\n\r\n"
            )?,
            Framing::Chunked => write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            )?,
            Framing::LyingChunked => write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            )?,
        }
        let chunked = !matches!(framing, Framing::Declared);
        let prefix = b"{\"content_uri\":\"mxc://test/accepted\",\"padding\":\"";
        let suffix = b"\"}";
        let mut position = 0;
        while position < TOTAL && !stop.load(Ordering::Acquire) {
            let n = bytes.len().min(TOTAL - position);
            bytes[..n].fill(b'a');
            if position == 0 {
                bytes[..prefix.len()].copy_from_slice(prefix);
            }
            if position + n == TOTAL {
                bytes[n - suffix.len()..n].copy_from_slice(suffix);
            }
            if chunked {
                write!(socket, "{n:x}\r\n")?;
            }
            socket.write_all(&bytes[..n])?;
            if chunked {
                socket.write_all(b"\r\n")?;
            }
            socket.flush()?;
            position += n;
            sent.fetch_add(n, Ordering::AcqRel);
            if position == 128 * 1024 {
                // New code closes/rejects here. Old bytes().await keeps
                // waiting, then gets the complete valid large
                // JSON after this timeout.
                let mut peer = [0; 1];
                match socket.read(&mut peer) {
                    Ok(0) => return Ok(()),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
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
        if chunked && position == TOTAL {
            socket.write_all(b"0\r\n\r\n")?;
        }
        return Ok(());
    }
    let (status, body) = if path.starts_with("/_matrix/client/v3/sync") {
        (
            200,
            json!({"next_batch":"response-fixture","rooms":{"join":{"!response:test":{"state":{"events":[{"type":"m.room.create","state_key":"","event_id":"$create","sender":"@example:localhost","origin_server_ts":1,"content":{"creator":"@example:localhost","room_version":"10"}},{"type":"m.room.member","state_key":"@example:localhost","event_id":"$join","sender":"@example:localhost","origin_server_ts":2,"content":{"membership":"join"}}]},"timeline":{"events":[],"limited":false,"prev_batch":"empty"},"ephemeral":{"events":[]},"account_data":{"events":[]},"unread_notifications":{}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}}),
        )
    } else if path.contains("/media/config") {
        (200, json!({"m.upload.size":128*1024*1024}))
    } else {
        (404, json!({"errcode":"M_NOT_FOUND","error":"actual fixture metadata missing"}))
    };
    let body = serde_json::to_vec(&body).unwrap();
    write!(
        socket,
        "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    socket.write_all(&body)?;
    socket.flush()
}
async fn bounded_response(framing: Framing) {
    let peer = Peer::start(framing);
    let governor = small();
    let client = MockClientBuilder::new(Some(&peer.url))
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    client.sync_once(crate::config::SyncSettings::default().timeout(Duration::ZERO)).await.unwrap();
    let room = client.get_room(ruma::room_id!("!response:test")).unwrap();
    let queue = room.send_queue();
    let (_, mut watch) = queue.subscribe().await.unwrap();
    queue
        .send_attachment("source", mime::IMAGE_JPEG, vec![7; 16], AttachmentConfig::new())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let RoomSendQueueUpdate::SendError { error, is_recoverable, .. } =
                watch.recv().await.unwrap()
            {
                assert!(!is_recoverable);
                assert!(
                    is_unknown_upload(&error),
                    "original oversized response must be unknown: {error}"
                );
                break;
            }
        }
    })
    .await
    .expect("bounded actual upload response");
    let rows = client.state_store().load_send_queue_requests(room.room_id()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].error, Some(QueueWedgeError::GenericApiError { .. })));
    assert!(
        peer.sent.load(Ordering::Acquire) < TOTAL,
        "peer was stopped before the full response body"
    );
    state(&governor, |s| s.jobs == 0).await;
}
#[tokio::test]
async fn declared_large_post_response_is_rejected_before_collection() {
    bounded_response(Framing::Declared).await;
}
#[tokio::test]
async fn chunked_large_post_response_is_bounded_by_actual_cumulative_chunks() {
    bounded_response(Framing::Chunked).await;
}
#[tokio::test]
async fn false_small_length_cannot_bypass_the_actual_chunked_post_response_limit() {
    bounded_response(Framing::LyingChunked).await;
}

#[tokio::test]
async fn default_client_keeps_existing_response_behavior_without_governed_owner() {
    let peer = Peer::start(Framing::Declared);
    let client = MockClientBuilder::new(Some(&peer.url)).build().await;
    let response = client.media().upload(&mime::IMAGE_JPEG, vec![7; 16], None).await.unwrap();
    assert_eq!(response.content_uri, ruma::mxc_uri!("mxc://test/accepted"));
    assert_eq!(peer.sent.load(Ordering::Acquire), TOTAL);
}

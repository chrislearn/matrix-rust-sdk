use std::{sync::atomic::AtomicU64, time::Duration};

use matrix_sdk_base::media::{MediaFormat, MediaRequestParameters};
use ruma::{api::MatrixVersion, events::room::MediaSource, owned_mxc_uri};

use super::*;
use crate::{Client, config::RequestConfig};
mod controlled_store;

async fn state(governor: &UploadGovernor, check: impl Fn(UploadStats) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check(governor.stats()) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("original governor transition");
}
fn small() -> Arc<UploadGovernor> {
    Arc::new(
        UploadGovernor::new(UploadLimits {
            max_file_bytes: 64,
            max_jobs: 2,
            max_pending_write_bytes: 128,
        })
        .unwrap(),
    )
}
fn request() -> MediaRequestParameters {
    MediaRequestParameters {
        source: MediaSource::Plain(owned_mxc_uri!("mxc://test/source")),
        format: MediaFormat::File,
    }
}
#[tokio::test]
async fn custody_original_sdk_future_survives_cancel_and_holds_client_until_real_return() {
    let governor = small();
    let (client, gate) = controlled_store::controlled_client().await;
    let weak = client.downgrade();
    let owner = governor.clone();
    let original = tokio::spawn({
        let weak = weak.clone();
        async move { owner.cache_length(weak, request()).await }
    });
    gate.started().await;
    original.abort();
    let _ = original.await;
    drop(client);
    assert!(weak.get().is_some(), "actual SDK future retains its original client lease");
    assert_eq!(governor.stats().active, 1);
    assert!(matches!(governor.shutdown_until(Instant::now()), UploadShutdown::Pending(_)));
    gate.release();
    state(&governor, |s| s.jobs == 0).await;
    assert!(weak.get().is_none());
    assert!(matches!(
        governor.shutdown_until(Instant::now() + Duration::from_secs(1)),
        UploadShutdown::Joined(_)
    ));
}
#[tokio::test]
async fn custody_write_payload_and_metadata_remain_bounded_after_observer_drop() {
    let governor = small();
    let (client, gate) = controlled_store::controlled_client().await;
    let weak = client.downgrade();
    let write = governor
        .submit(64, move |custody, _| async move {
            let client = weak.get().ok_or(UploadGovernorError::Paused)?;
            custody.retain_client(&client);
            let store =
                client.media_store().lock().await.map_err(|_| UploadGovernorError::WorkerFailed)?;
            store
                .add_media_content(
                    &request(),
                    vec![7; 64],
                    matrix_sdk_base::media::store::IgnoreMediaRetentionPolicy::Yes,
                )
                .await
                .map_err(|_| UploadGovernorError::WorkerFailed)
        })
        .unwrap();
    gate.started().await;
    drop(write);
    let queued =
        governor.submit::<(), UploadGovernorError, _, _>(64, |_, _| async { Ok(()) }).unwrap();
    assert_eq!(governor.stats().pending_write_bytes, 128);
    assert!(matches!(
        governor.submit::<(), UploadGovernorError, _, _>(0, |_, _| async { Ok(()) }),
        Err(UploadGovernorError::Capacity)
    ));
    drop(queued);
    assert_eq!(governor.stats().jobs, 2);
    gate.release();
    state(&governor, |s| s.jobs == 0).await;
    assert_eq!(governor.stats().pending_write_bytes, 0);
}
#[tokio::test]
async fn custody_actual_serialized_bytes_last_clone_keeps_original_slot_and_client() {
    let governor = small();
    let client = Client::builder()
        .homeserver_url("http://127.0.0.1:1")
        .server_versions([MatrixVersion::V1_15])
        .build()
        .await
        .unwrap();
    let weak = client.downgrade();
    let job = governor
        .submit::<_, crate::Error, _, _>(0, {
            let weak = weak.clone();
            move |custody, _| async move {
                let client = weak.get().unwrap();
                custody.retain_client(&client);
                let mut http = client.inner.http_client.clone();
                http.body_owner = Some(crate::http_client::BodyOwner {
                    _custody: custody,
                    admissions: Arc::new(AtomicU64::new(0)),
                });
                let request =
                    ruma::api::client::media::create_content::v3::Request::new(vec![9; 64]);
                let request = http
                    .serialize_request(
                        request,
                        RequestConfig::new(),
                        "http://localhost".into(),
                        Some("fixture-token"),
                        std::borrow::Cow::Owned(ruma::api::SupportedVersions {
                            versions: [MatrixVersion::V1_15].into(),
                            features: Default::default(),
                        }),
                    )
                    .unwrap();
                Ok(request.into_body())
            }
        })
        .unwrap();
    let body = job.await.unwrap();
    let last = body.clone();
    drop(body);
    drop(client);
    assert!(weak.get().is_some());
    assert_eq!(governor.stats().active, 1);
    let queued =
        governor.submit::<(), UploadGovernorError, _, _>(0, |_, _| async { Ok(()) }).unwrap();
    assert_eq!(governor.stats().queued, 1);
    assert_eq!(last.as_ref(), &[9; 64]);
    drop(last);
    queued.await.unwrap();
    state(&governor, |s| s.jobs == 0).await;
    assert!(weak.get().is_none());
}
#[tokio::test]
async fn custody_unknown_original_panic_poison_retains_lease_and_budget() {
    let governor = small();
    let client = Client::builder()
        .homeserver_url("http://127.0.0.1:1")
        .server_versions([MatrixVersion::V1_15])
        .build()
        .await
        .unwrap();
    let weak = client.downgrade();
    let op = governor
        .submit::<(), UploadGovernorError, _, _>(64, {
            let weak = weak.clone();
            move |custody, _| async move {
                let client = weak.get().unwrap();
                custody.retain_client(&client);
                panic!("controlled original worker failure");
            }
        })
        .unwrap();
    assert_eq!(op.await, Err(UploadGovernorError::WorkerFailed));
    drop(client);
    state(&governor, |s| s.unknown == 1).await;
    assert!(weak.get().is_some());
    assert_eq!(governor.stats().pending_write_bytes, 64);
    assert!(matches!(governor.shutdown_until(Instant::now()), UploadShutdown::Pending(_)));
    assert!(matches!(
        governor.submit::<(), UploadGovernorError, _, _>(0, |_, _| async { Ok(()) }),
        Err(UploadGovernorError::WorkerFailed)
    ));
}
mod compatibility;
mod metadata;
mod outbox;
mod responses;

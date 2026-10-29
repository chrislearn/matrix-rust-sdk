use ruma::room_id;
use wiremock::ResponseTemplate;

use super::*;
use crate::{HttpRequestFence, test_utils::mocks::MatrixMockServer};

#[tokio::test]
async fn no_governor_remains_default_and_clients_can_share_one_policy() {
    let mock = MatrixMockServer::new().await;
    let default = mock.client_builder().build().await;
    assert!(default.send_queue().upload_governor().is_none());
    let governor = small();
    let a = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    let b = mock
        .client_builder()
        .on_builder(|builder| builder.upload_governor(governor.clone()))
        .build()
        .await;
    assert!(Arc::ptr_eq(
        &a.send_queue().upload_governor().unwrap(),
        &b.send_queue().upload_governor().unwrap()
    ));
}
async fn fence_upload(retire_after_attempt: bool) -> (crate::Error, usize) {
    let mock = MatrixMockServer::new().await;
    let governor = small();
    let fence = Arc::new(HttpRequestFence::new(|| true));
    let client = mock
        .client_builder()
        .on_builder(|builder| {
            builder.upload_governor(governor.clone()).request_fence(fence.clone())
        })
        .build()
        .await;
    let room = mock.sync_joined_room(&client, room_id!("!fence:test")).await;
    client
        .media_store()
        .lock()
        .await
        .unwrap()
        .add_media_content(
            &request(),
            vec![7; 16],
            matrix_sdk_base::media::store::IgnoreMediaRetentionPolicy::Yes,
        )
        .await
        .unwrap();
    mock.mock_room_state_encryption().plain().mount().await;
    mock.mock_authenticated_media_config().ok_default().mount().await;
    // Warm the same actual SDK cached upload config before retirement so the
    // error under test is actual media POST admission, not a preceding GET.
    client.load_or_fetch_max_upload_size().await.unwrap();
    room.latest_encryption_state().await.unwrap();
    let count = Arc::new(AtomicU64::new(0));
    if retire_after_attempt {
        mock.mock_upload()
            .respond_with({
                let fence = fence.clone();
                let count = count.clone();
                move |_: &wiremock::Request| {
                    count.fetch_add(1, Ordering::AcqRel);
                    fence.retire();
                    ResponseTemplate::new(503).set_body_json(
                        serde_json::json!({"errcode":"M_UNKNOWN","error":"transient fixture"}),
                    )
                }
            })
            .mock_once()
            .mount()
            .await;
    } else {
        fence.retire();
    }
    let error = governor
        .submit(0, move |custody, live| async move {
            upload_original(&room, request(), "image/jpeg".into(), None, 64, custody, live).await
        })
        .unwrap()
        .await
        .unwrap_err();
    (error, count.load(Ordering::Acquire) as usize)
}
#[tokio::test]
async fn before_first_admission_is_paused_but_after_attempt_retirement_is_unknown() {
    let (error, count) = fence_upload(false).await;
    assert!(matches!(error, crate::Error::UploadGovernor(UploadGovernorError::Paused)));
    assert_eq!(count, 0);
    let (error, count) = fence_upload(true).await;
    assert!(matches!(error, crate::Error::UploadGovernor(UploadGovernorError::OutcomeUnknown)));
    assert_eq!(count, 1);
}

#[tokio::test]
async fn actual_governed_upload_keeps_client_wide_fence_and_automatic_401_refresh() {
    use wiremock::{
        Mock,
        matchers::{method, path},
    };

    use crate::{SessionMeta, SessionTokens, authentication::matrix::MatrixSession};
    let mock = MatrixMockServer::new().await;
    let governor = small();
    let fence = Arc::new(HttpRequestFence::new(|| true));
    let client = mock
        .client_builder()
        .unlogged()
        .on_builder(|builder| {
            builder.upload_governor(governor.clone()).handle_refresh_tokens().request_fence(fence)
        })
        .build()
        .await;
    client
        .matrix_auth()
        .restore_session(
            MatrixSession {
                meta: SessionMeta {
                    user_id: ruma::user_id!("@refresh:test").to_owned(),
                    device_id: ruma::device_id!("DEVICE").to_owned(),
                },
                tokens: SessionTokens {
                    access_token: "original-access".into(),
                    refresh_token: Some("original-refresh".into()),
                },
            },
            Default::default(),
        )
        .await
        .unwrap();
    let room = mock.sync_joined_room(&client, room_id!("!refresh:test")).await;
    client
        .media_store()
        .lock()
        .await
        .unwrap()
        .add_media_content(
            &request(),
            vec![7; 16],
            matrix_sdk_base::media::store::IgnoreMediaRetentionPolicy::Yes,
        )
        .await
        .unwrap();
    mock.mock_room_state_encryption().expect_access_token("original-access").plain().mount().await;
    mock.mock_authenticated_media_config().ok_default().mount().await;
    Mock::given(method("POST")).and(path("/_matrix/client/v3/refresh")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token":"new-access","refresh_token":"new-refresh","expires_in_ms":3600000}))).expect(1).mount(&mock).await;
    let attempts = Arc::new(AtomicU64::new(0));
    mock.mock_upload().respond_with({let attempts=attempts.clone();move |request:&wiremock::Request| {
        assert_eq!(request.body,[7;16]);
        if attempts.fetch_add(1,Ordering::AcqRel)==0 {
            assert_eq!(request.headers.get("authorization").unwrap(),"Bearer original-access");
            ResponseTemplate::new(401).set_body_json(serde_json::json!({"errcode":"M_UNKNOWN_TOKEN","error":"fixture expired access","soft_logout":false}))
        } else {
            assert_eq!(request.headers.get("authorization").unwrap(),"Bearer new-access");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"content_uri":"mxc://test/refreshed"}))
        }
    }}).expect(2).mount().await;
    let source = governor
        .submit(0, move |custody, live| async move {
            upload_original(&room, request(), "image/jpeg".into(), None, 64, custody, live).await
        })
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(source,MediaSource::Plain(uri) if uri == ruma::owned_mxc_uri!("mxc://test/refreshed"))
    );
    assert_eq!(attempts.load(Ordering::Acquire), 2);
}

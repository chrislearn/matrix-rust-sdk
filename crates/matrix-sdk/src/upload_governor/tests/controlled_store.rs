//! Controlled original SDK futures, delegating all real content operations to
//! MemoryMediaStore. This is not a SQLite blocking-IO measurement.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use matrix_sdk_base::{
    cross_process_lock::{CrossProcessLockConfig, CrossProcessLockGeneration},
    media::{
        MediaRequestParameters,
        store::{
            BoundedMediaStoreError, IgnoreMediaRetentionPolicy, MediaRetentionPolicy, MediaStore,
            MediaStoreError, MemoryMediaStore,
        },
    },
};

use super::*;

#[derive(Debug)]
pub(super) struct Gate {
    pub entered: AtomicUsize,
    blocked: AtomicBool,
    released: tokio::sync::Semaphore,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            blocked: AtomicBool::new(true),
            released: tokio::sync::Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        if self.blocked.load(Ordering::Acquire) {
            self.entered.fetch_add(1, Ordering::Release);
            self.released.acquire().await.unwrap().forget();
        }
    }
    pub async fn started(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.entered.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("actual SDK MediaStore get/add future entered");
    }
    pub fn release(&self) {
        self.blocked.store(false, Ordering::Release);
        self.released.add_permits(1);
    }
}
#[derive(Clone, Debug)]
struct ControlledStore {
    inner: MemoryMediaStore,
    gate: Arc<Gate>,
}
#[async_trait::async_trait]
impl MediaStore for ControlledStore {
    type Error = MediaStoreError;
    async fn try_take_leased_lock(
        &self,
        lease_duration_ms: u32,
        key: &str,
        holder: &str,
    ) -> Result<Option<CrossProcessLockGeneration>, Self::Error> {
        self.inner.try_take_leased_lock(lease_duration_ms, key, holder).await
    }
    async fn add_media_content(
        &self,
        request: &MediaRequestParameters,
        content: Vec<u8>,
        ignore_policy: IgnoreMediaRetentionPolicy,
    ) -> Result<(), Self::Error> {
        self.gate.wait().await;
        self.inner.add_media_content(request, content, ignore_policy).await
    }
    async fn get_media_content_bounded(
        &self,
        request: &MediaRequestParameters,
        max: usize,
    ) -> Result<Option<Vec<u8>>, BoundedMediaStoreError> {
        self.gate.wait().await;
        self.inner.get_media_content_bounded(request, max).await
    }
    async fn replace_media_key(
        &self,
        from: &MediaRequestParameters,
        to: &MediaRequestParameters,
    ) -> Result<(), Self::Error> {
        self.inner.replace_media_key(from, to).await
    }
    async fn get_media_content(
        &self,
        request: &MediaRequestParameters,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get_media_content(request).await
    }
    async fn remove_media_content(
        &self,
        request: &MediaRequestParameters,
    ) -> Result<(), Self::Error> {
        self.inner.remove_media_content(request).await
    }
    async fn remove_media_content_for_uri(&self, uri: &ruma::MxcUri) -> Result<(), Self::Error> {
        self.inner.remove_media_content_for_uri(uri).await
    }
    async fn set_media_retention_policy(
        &self,
        policy: MediaRetentionPolicy,
    ) -> Result<(), Self::Error> {
        self.inner.set_media_retention_policy(policy).await
    }
    fn media_retention_policy(&self) -> MediaRetentionPolicy {
        self.inner.media_retention_policy()
    }
    async fn set_ignore_media_retention_policy(
        &self,
        request: &MediaRequestParameters,
        policy: IgnoreMediaRetentionPolicy,
    ) -> Result<(), Self::Error> {
        self.inner.set_ignore_media_retention_policy(request, policy).await
    }
    async fn clean(&self) -> Result<(), Self::Error> {
        self.inner.clean().await
    }
    async fn close(&self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
    async fn reopen(&self) -> Result<(), Self::Error> {
        self.inner.reopen().await
    }
    async fn optimize(&self) -> Result<(), Self::Error> {
        self.inner.optimize().await
    }
    async fn get_size(&self) -> Result<Option<usize>, Self::Error> {
        self.inner.get_size().await
    }
}
pub(super) async fn controlled_client() -> (Client, Arc<Gate>) {
    let gate = Gate::new();
    let inner = MemoryMediaStore::new();
    // The production SDK default drops files over 20MiB. This fixture must
    // retain its 32MiB payload to verify original completion after
    // cancellation.
    inner.set_media_retention_policy(MediaRetentionPolicy::empty()).await.unwrap();
    let store = ControlledStore { inner, gate: gate.clone() };
    let config =
        crate::config::StoreConfig::new(CrossProcessLockConfig::SingleProcess).media_store(store);
    let client = Client::builder()
        .homeserver_url("http://127.0.0.1:1")
        .server_versions([MatrixVersion::V1_11])
        .store_config(config)
        .build()
        .await
        .unwrap();
    (client, gate)
}

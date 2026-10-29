use super::*;
use crate::media::{MediaFormat, store::MemoryMediaStore};

// This implements only the pre-existing API, as an external backend would.
// None of its methods may be invoked by the default bounded implementation.
#[derive(Debug)]
struct LegacyStore;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl MediaStore for LegacyStore {
    type Error = MediaStoreError;
    async fn try_take_leased_lock(
        &self,
        _: u32,
        _: &str,
        _: &str,
    ) -> Result<Option<CrossProcessLockGeneration>, Self::Error> {
        unreachable!()
    }
    async fn add_media_content(
        &self,
        _: &MediaRequestParameters,
        _: Vec<u8>,
        _: IgnoreMediaRetentionPolicy,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn replace_media_key(
        &self,
        _: &MediaRequestParameters,
        _: &MediaRequestParameters,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn get_media_content(
        &self,
        _: &MediaRequestParameters,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        panic!("the bounded default must never call the unbounded API")
    }
    async fn remove_media_content(&self, _: &MediaRequestParameters) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn remove_media_content_for_uri(&self, _: &MxcUri) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn set_media_retention_policy(&self, _: MediaRetentionPolicy) -> Result<(), Self::Error> {
        unreachable!()
    }
    fn media_retention_policy(&self) -> MediaRetentionPolicy {
        unreachable!()
    }
    async fn set_ignore_media_retention_policy(
        &self,
        _: &MediaRequestParameters,
        _: IgnoreMediaRetentionPolicy,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn clean(&self) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn close(&self) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn reopen(&self) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn optimize(&self) -> Result<(), Self::Error> {
        unreachable!()
    }
    async fn get_size(&self) -> Result<Option<usize>, Self::Error> {
        unreachable!()
    }
}

#[matrix_sdk_test::async_test]
async fn test_bounded_media_default_is_explicitly_unsupported_without_unbounded_fallback() {
    let request = MediaRequestParameters {
        source: ruma::events::room::MediaSource::Plain(
            ruma::mxc_uri!("mxc://media.test/unsupported").to_owned(),
        ),
        format: MediaFormat::File,
    };
    assert!(matches!(
        LegacyStore.get_media_content_bounded(&request, 1).await,
        Err(BoundedMediaStoreError::Unsupported)
    ));
    assert!(matches!(
        LegacyStore.into_media_store().get_media_content_bounded(&request, 1).await,
        Err(BoundedMediaStoreError::Unsupported)
    ));
    // A supported empty backend returns a real miss through the erased facade.
    assert_eq!(
        MemoryMediaStore::new()
            .into_media_store()
            .get_media_content_bounded(&request, 0)
            .await
            .unwrap(),
        None
    );
}

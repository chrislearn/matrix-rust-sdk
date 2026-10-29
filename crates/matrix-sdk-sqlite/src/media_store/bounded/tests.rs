use matrix_sdk_base::media::{MediaFormat, store::IntoMediaStore};
use ruma::{events::room::MediaSource, mxc_uri};
use tempfile::TempDir;

use super::*;

fn request() -> MediaRequestParameters {
    MediaRequestParameters {
        source: MediaSource::Plain(mxc_uri!("mxc://media.test/local-cache").to_owned()),
        format: MediaFormat::File,
    }
}

async fn store(encrypted: bool) -> (TempDir, SqliteMediaStore) {
    let directory = tempfile::tempdir().unwrap();
    let key = [7; 32];
    let store =
        SqliteMediaStore::open_with_key(directory.path(), encrypted.then_some(key.as_slice()))
            .await
            .unwrap();
    (directory, store)
}

async fn overwrite(store: &SqliteMediaStore, data: Vec<u8>) {
    let request = request();
    let uri = store.encode_key(keys::MEDIA, request.source.unique_key());
    let format = store.encode_key(keys::MEDIA, request.format.unique_key());
    store
        .write()
        .await
        .unwrap()
        .execute("UPDATE media SET data = ? WHERE uri = ? AND format = ?", (data, uri, format))
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_media_sqlite_plain_encrypted_limits_legacy_api_and_erasure() {
    for encrypted in [false, true] {
        let (_directory, store) = store(encrypted).await;
        let request = request();
        assert_eq!(store.get_media_content_bounded(&request, 0).await.unwrap(), None);
        for size in [0, 1, 255, 256, 65_535] {
            let bytes = vec![255; size];
            store
                .add_media_content(&request, bytes.clone(), IgnoreMediaRetentionPolicy::Yes)
                .await
                .unwrap();
            assert_eq!(
                store.get_media_content_bounded(&request, size).await.unwrap(),
                Some(bytes.clone())
            );
            assert_eq!(
                store.get_media_content_bounded(&request, size + 1).await.unwrap(),
                Some(bytes.clone())
            );
            if size > 0 {
                assert!(
                    matches!(store.get_media_content_bounded(&request, size - 1).await, Err(BoundedMediaStoreError::Exceeded { max_bytes }) if max_bytes == size - 1)
                );
            }
            // Existing callers still receive the original content, and error
            // erasure must delegate the new method instead of using its default.
            assert_eq!(store.get_media_content(&request).await.unwrap(), Some(bytes.clone()));
            assert_eq!(
                store
                    .clone()
                    .into_media_store()
                    .get_media_content_bounded(&request, size)
                    .await
                    .unwrap(),
                Some(bytes)
            );
        }
        assert!(matches!(
            store.get_media_content_bounded(&request, usize::MAX).await,
            Err(BoundedMediaStoreError::InvalidLimit { .. })
        ));
        store.close().await.unwrap();
        assert!(matches!(
            store.get_media_content_bounded(&request, 100_000).await,
            Err(BoundedMediaStoreError::Store(_))
        ));
        store.reopen().await.unwrap();
        assert_eq!(store.get_media_content(&request).await.unwrap().unwrap().len(), 65_535);
    }
}

#[tokio::test]
async fn bounded_media_sqlite_rejects_encoded_growth_before_copy_or_decode() {
    let (_directory, store) = store(true).await;
    let request = request();
    store.add_media_content(&request, vec![1], IgnoreMediaRetentionPolicy::Yes).await.unwrap();
    // A malformed oversized blob must be rejected by SQL length, not by a
    // full-buffer MessagePack decoder or the AEAD implementation.
    overwrite(&store, vec![255; 10_000]).await;
    assert!(matches!(
        store.get_media_content_bounded(&request, 16).await,
        Err(BoundedMediaStoreError::Exceeded { max_bytes: 16 })
    ));
    overwrite(&store, vec![255]).await;
    assert!(matches!(
        store.get_media_content_bounded(&request, 16).await,
        Err(BoundedMediaStoreError::Store(_))
    ));
    // A small envelope can still contain a plaintext just over the limit. An
    // invalid tag must not get as far as decryption when its length is too big.
    let cipher = store.store_cipher.as_ref().unwrap();
    let mut encoded =
        rmp_serde::to_vec_named(&cipher.encrypt_value_data(vec![1; 17]).unwrap()).unwrap();
    assert!(encoded.len() <= 16 + ENCRYPTED_OVERHEAD);
    // Read a valid envelope at both limits; its implied plaintext is 17 bytes.
    overwrite(&store, encoded.clone()).await;
    assert!(matches!(
        store.get_media_content_bounded(&request, 16).await,
        Err(BoundedMediaStoreError::Exceeded { max_bytes: 16 })
    ));
    assert_eq!(store.get_media_content_bounded(&request, 17).await.unwrap(), Some(vec![1; 17]));
    // An in-bound but unauthentic ciphertext is a backend/integrity failure,
    // never a cache miss or successful plaintext.
    let ciphertext = encoded.iter().position(|byte| *byte == 0xc4).unwrap() + 2;
    encoded[ciphertext] ^= 1;
    overwrite(&store, encoded).await;
    assert!(matches!(
        store.get_media_content_bounded(&request, 16).await,
        Err(BoundedMediaStoreError::Exceeded { max_bytes: 16 })
    ));
    assert!(matches!(
        store.get_media_content_bounded(&request, 17).await,
        Err(BoundedMediaStoreError::Store(MediaStoreError::Encryption(_)))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_media_sqlite_concurrent_replacement_cannot_cross_the_length_check() {
    for encrypted in [false, true] {
        let (_directory, store) = store(encrypted).await;
        let request = request();
        store
            .add_media_content(&request, vec![1; 32], IgnoreMediaRetentionPolicy::Yes)
            .await
            .unwrap();
        let reader_store = store.clone();
        let (entered, at_check) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let reader = tokio::spawn(async move {
            reader_store
                .read_content_bounded(&request, 32, move || {
                    entered.send(()).unwrap();
                    wait.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), at_check).await.unwrap().unwrap();
        // Use another actual pooled SQLite connection, bypassing the store's
        // shared Rust write mutex. Only the SQL transaction can exclude it.
        let writer_conn = store.read().await.unwrap();
        let request = self::request();
        let uri = store.encode_key(keys::MEDIA, request.source.unique_key());
        let format = store.encode_key(keys::MEDIA, request.format.unique_key());
        let encoded = store.encode_value(vec![2; 4096]).unwrap();
        let (started, writing) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            started.send(()).unwrap();
            writer_conn
                .execute(
                    "UPDATE media SET data = ? WHERE uri = ? AND format = ?",
                    (encoded, uri, format),
                )
                .await
        });
        writing.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!writer.is_finished(), "the original bounded read must retain its SQL entry");
        release.send(()).unwrap();
        assert_eq!(reader.await.unwrap().unwrap(), Some(vec![1; 32]));
        writer.await.unwrap().unwrap();
        assert!(matches!(
            store.get_media_content_bounded(&request, 32).await,
            Err(BoundedMediaStoreError::Exceeded { .. })
        ));
        assert_eq!(store.get_media_content(&request).await.unwrap(), Some(vec![2; 4096]));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_media_sqlite_cancelled_observer_does_not_finish_original_blocking_io() {
    let (_directory, store) = store(false).await;
    let request = request();
    store.add_media_content(&request, vec![1; 32], IgnoreMediaRetentionPolicy::Yes).await.unwrap();
    let reader_store = store.clone();
    let (entered, at_check) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let reader = tokio::spawn(async move {
        reader_store
            .read_content_bounded(&request, 32, move || {
                entered.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), at_check).await.unwrap().unwrap();
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    // Cancellation has finished at the async observer but the original SQL
    // closure still owns the transaction. This is why callers must retain their
    // scratch/operation permit with a shielded original worker, not an observer.
    let writer_conn = store.read().await.unwrap();
    let request = self::request();
    let uri = store.encode_key(keys::MEDIA, request.source.unique_key());
    let format = store.encode_key(keys::MEDIA, request.format.unique_key());
    let writer = tokio::spawn(async move {
        writer_conn
            .execute(
                "UPDATE media SET data = ? WHERE uri = ? AND format = ?",
                (vec![2; 4096], uri, format),
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "dropping an observer must not masquerade as original IO completion"
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        store.get_media_content_bounded(&request, 32).await,
        Err(BoundedMediaStoreError::Exceeded { .. })
    ));
}

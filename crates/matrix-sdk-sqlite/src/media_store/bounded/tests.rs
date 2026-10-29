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

// SQLite's allocator counters are process-global. Run the real store path in a
// fresh, exact-test child so unrelated parallel tests cannot contaminate them.
#[tokio::test]
async fn bounded_media_sqlite_oversize_preflight_avoids_native_blob_allocation() {
    const CHILD: &str = "HAGENCY_SQLITE_BOUNDED_NATIVE_PROBE";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "media_store::bounded::tests::bounded_media_sqlite_oversize_preflight_avoids_native_blob_allocation",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "native probe failed: {stdout}\n{stderr}");
        assert!(stdout.contains("1 passed"), "native probe must execute: {stdout}\n{stderr}");
        return;
    }

    const BLOB_BYTES: usize = 128 * 1024 * 1024;
    const MAX_NATIVE_GROWTH: i64 = 16 * 1024 * 1024;
    for encrypted in [false, true] {
        let (_directory, store) = store(encrypted).await;
        let request = request();
        store.add_media_content(&request, vec![1], IgnoreMediaRetentionPolicy::Yes).await.unwrap();
        let uri = store.encode_key(keys::MEDIA, request.source.unique_key());
        let format = store.encode_key(keys::MEDIA, request.format.unique_key());
        let conn = store.write().await.unwrap();
        conn.execute_batch("PRAGMA cache_size=-64; PRAGMA mmap_size=0;").await.unwrap();
        // zeroblob creates a large existing entry without a Rust content Vec.
        // For the encrypted store this is deliberately malformed at rest: the
        // size preflight must reject it before either native copy or decoding.
        conn.execute(
            "UPDATE media SET data=zeroblob(?) WHERE uri=? AND format=?",
            (BLOB_BYTES, uri, format),
        )
        .await
        .unwrap();
        conn.execute_batch(
            "CREATE TRIGGER forbid_access_update BEFORE UPDATE OF last_access ON media \
             BEGIN SELECT RAISE(ABORT, 'oversize entry reached UPDATE'); END;",
        )
        .await
        .unwrap();
        drop(conn);
        assert!(matches!(
            store.get_media_content_bounded(&request, 16).await,
            Err(BoundedMediaStoreError::Exceeded { max_bytes: 16 })
        ));
        let conn = store.write().await.unwrap();
        conn.execute_batch("DROP TRIGGER forbid_access_update;").await.unwrap();
        drop(conn);

        // The trigger is absent for this measurement: it cannot hide an
        // allocating UPDATE. These SQLite APIs have no pointers/preconditions
        // and are safe concurrently, but isolation makes the counter specific.
        let baseline = unsafe { rusqlite::ffi::sqlite3_memory_used() };
        unsafe { rusqlite::ffi::sqlite3_memory_highwater(1) };
        assert!(matches!(
            store.get_media_content_bounded(&request, 16).await,
            Err(BoundedMediaStoreError::Exceeded { max_bytes: 16 })
        ));
        let peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) };
        assert!(baseline > 0, "SQLite allocator instrumentation must be active");
        let growth = peak.saturating_sub(baseline);
        eprintln!("encrypted={encrypted} existing_bytes={BLOB_BYTES} native_growth={growth}");
        assert!(
            growth < MAX_NATIVE_GROWTH,
            "rejecting a 128MiB entry must not allocate its native blob: {growth} bytes"
        );
        // SQLite's BLOB affinity also permits a corrupted TEXT entry.
        // Its NUL character length is zero despite 128MiB of encoded bytes.
        let conn = store.write().await.unwrap();
        conn.execute_batch(
            "UPDATE media SET data=CAST(data AS TEXT); \
             CREATE TRIGGER forbid_access_update BEFORE UPDATE OF last_access ON media \
             BEGIN SELECT RAISE(ABORT, 'non-blob entry reached UPDATE'); END;",
        )
        .await
        .unwrap();
        drop(conn);
        assert!(matches!(
            store.get_media_content_bounded(&request, 16).await,
            Err(BoundedMediaStoreError::Store(MediaStoreError::InvalidData { .. }))
        ));
        let conn = store.write().await.unwrap();
        conn.execute_batch("DROP TRIGGER forbid_access_update;").await.unwrap();
        drop(conn);
        let baseline = unsafe { rusqlite::ffi::sqlite3_memory_used() };
        unsafe { rusqlite::ffi::sqlite3_memory_highwater(1) };
        assert!(matches!(
            store.get_media_content_bounded(&request, 16).await,
            Err(BoundedMediaStoreError::Store(MediaStoreError::InvalidData { .. }))
        ));
        let peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) };
        let growth = peak.saturating_sub(baseline);
        eprintln!("encrypted={encrypted} invalid_text_bytes={BLOB_BYTES} native_growth={growth}");
        assert!(growth < MAX_NATIVE_GROWTH, "invalid TEXT must not materialize: {growth} bytes");

        // Also record the accepted 64MiB path, including the native UPDATE
        // buffers. This is an observation, not a two-times process/RSS cap.
        const ACCEPTED_BYTES: usize = 64 * 1024 * 1024;
        store
            .add_media_content(&request, vec![1; ACCEPTED_BYTES], IgnoreMediaRetentionPolicy::Yes)
            .await
            .unwrap();
        let baseline = unsafe { rusqlite::ffi::sqlite3_memory_used() };
        unsafe { rusqlite::ffi::sqlite3_memory_highwater(1) };
        let accepted =
            store.get_media_content_bounded(&request, ACCEPTED_BYTES).await.unwrap().unwrap();
        let peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) };
        assert_eq!(accepted.len(), ACCEPTED_BYTES);
        assert!(accepted.iter().all(|byte| *byte == 1));
        eprintln!(
            "encrypted={encrypted} accepted_bytes={ACCEPTED_BYTES} native_growth={}",
            peak.saturating_sub(baseline)
        );
        store.close().await.unwrap();
    }
}

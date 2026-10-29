//! Allocation-bounded reads of the original media entry.
use matrix_sdk_base::media::store::{BoundedMediaStoreError, MediaStoreError};

use super::*;

// Canonical rmp_serde EncryptedValue: three named fields, a binary ciphertext
// (plaintext + 16-byte AEAD tag), and a 24-byte nonce. Even when every nonce
// byte needs two MessagePack bytes, all framing and the tag fit within 128.
const ENCRYPTED_OVERHEAD: usize = 128;

enum ReadError {
    Sqlite(rusqlite::Error),
    Bound(BoundedMediaStoreError),
}
impl From<rusqlite::Error> for ReadError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}
impl From<ReadError> for BoundedMediaStoreError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Sqlite(error) => Self::Store(Error::from(error).into()),
            ReadError::Bound(error) => error,
        }
    }
}

impl SqliteMediaStore {
    // The callback is a no-op in production. Tests pause after metadata is
    // checked, while retaining this actual transaction, to race another writer.
    pub(super) async fn read_content_bounded(
        &self,
        request: &MediaRequestParameters,
        max_bytes: usize,
        after_size_check: impl FnOnce() + Send + 'static,
    ) -> std::result::Result<Option<Vec<u8>>, BoundedMediaStoreError> {
        let overhead = if self.store_cipher.is_some() { ENCRYPTED_OVERHEAD } else { 0 };
        let max_encoded = max_bytes
            .checked_add(overhead)
            .filter(|limit| i64::try_from(*limit).is_ok())
            .ok_or(BoundedMediaStoreError::InvalidLimit { max_bytes })?;
        let uri = self.encode_key(keys::MEDIA, request.source.unique_key());
        let format = self.encode_key(keys::MEDIA, request.format.unique_key());
        let timestamp = time_to_timestamp(SystemTime::now());
        let conn =
            self.write().await.map_err(|error| BoundedMediaStoreError::Store(error.into()))?;
        let bytes = conn
            .with_transaction::<_, ReadError, _>(move |txn| {
                // Preflight before UPDATE as well as before copying data:
                // SQLite can rebuild the entire row while updating last_access,
                // allocating native buffers proportional to the original blob.
                // BLOB affinity is not a strict type constraint. Reject TEXT
                // without evaluating its length (NUL/character semantics) or
                // casting/materializing it. CASE evaluates only its chosen arm.
                let size: Option<i64> = txn
                    .query_row(
                        "SELECT CASE WHEN typeof(data) = 'blob' THEN length(data) ELSE -1 END \
                         FROM media WHERE uri = ? AND format = ?",
                        (&uri, &format),
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(size) = size else {
                    return Ok(None);
                };
                let size = usize::try_from(size).map_err(|_| {
                    ReadError::Bound(
                        MediaStoreError::InvalidData {
                            details: "Cached media has an invalid encoded type or size".into(),
                        }
                        .into(),
                    )
                })?;
                if size > max_encoded {
                    return Err(ReadError::Bound(BoundedMediaStoreError::Exceeded { max_bytes }));
                }
                // This same transaction retains the preflight snapshot. Once
                // the write upgrade succeeds, no other connection can replace
                // it before the copy; a conflicting upgrade returns an error.
                txn.execute(
                    "UPDATE media SET last_access = ? WHERE uri = ? AND format = ?",
                    (timestamp, &uri, &format),
                )?;
                after_size_check();
                let bytes: Vec<u8> = txn.query_row(
                    "SELECT data FROM media WHERE uri = ? AND format = ?",
                    (&uri, &format),
                    |row| row.get(0),
                )?;
                if bytes.len() != size || bytes.len() > max_encoded {
                    return Err(ReadError::Bound(
                        MediaStoreError::InvalidData {
                            details: "Cached media changed during its bounded transaction".into(),
                        }
                        .into(),
                    ));
                }
                Ok(Some(bytes))
            })
            .await
            .map_err(BoundedMediaStoreError::from)?;
        drop(conn);
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let Some(cipher) = self.store_cipher.as_ref() else {
            return Ok(Some(bytes));
        };
        // The decoder borrows the MessagePack ciphertext bytes directly.
        // It refuses sequence size hints and checks plaintext length before
        // allocating the bounded copy that AEAD then decrypts in place.
        let plaintext =
            cipher.decrypt_value_data_bounded_msgpack(&bytes, max_bytes).map_err(|error| {
                match error {
                    matrix_sdk_store_encryption::Error::Length(limit, actual)
                        if limit == max_bytes && actual > max_bytes =>
                    {
                        BoundedMediaStoreError::Exceeded { max_bytes }
                    }
                    error => BoundedMediaStoreError::Store(MediaStoreError::Encryption(error)),
                }
            })?;
        Ok(Some(plaintext))
    }
}

#[cfg(test)]
mod tests;

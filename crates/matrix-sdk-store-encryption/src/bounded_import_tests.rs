// Copyright 2026 The Matrix.org Foundation C.I.C.
// SPDX-License-Identifier: Apache-2.0
use super::*;

#[test]
fn bounded_import_original_msgpack_and_legacy_json_preserve_keys() {
    let original = StoreCipher::new().unwrap();
    let messagepack = original.export("original-passphrase").unwrap();
    let parsed: EncryptedStoreCipher = rmp_serde::from_slice(&messagepack).unwrap();
    assert!(matches!(parsed.kdf_info, KdfInfo::Pbkdf2ToChaCha20Poly1305 { rounds: 200_000, .. }));
    let legacy_json = serde_json::to_vec(&parsed).unwrap();
    for encoded in [&messagepack, &legacy_json] {
        let bounded =
            StoreCipher::import_with_round_limit("original-passphrase", encoded, 1_000_000)
                .unwrap();
        let legacy = StoreCipher::import("original-passphrase", encoded).unwrap();
        assert_eq!(
            bounded.hash_key("proof", b"original data"),
            original.hash_key("proof", b"original data")
        );
        assert_eq!(
            legacy.hash_key("proof", b"original data"),
            original.hash_key("proof", b"original data")
        );
        assert!(matches!(
            StoreCipher::import_with_round_limit("wrong", encoded, 1_000_000),
            Err(Error::Encryption(_))
        ));
    }
}

#[test]
fn bounded_import_maximum_rounds_are_rejected_before_derivation() {
    // A real export is modified only in its pre-authentication cost field.
    // Without the limit, importing this blob would attempt billions of rounds.
    let original = StoreCipher::new().unwrap();
    let encoded = original._insecure_export_fast_for_testing("original-passphrase").unwrap();
    let mut parsed: EncryptedStoreCipher = rmp_serde::from_slice(&encoded).unwrap();
    let KdfInfo::Pbkdf2ToChaCha20Poly1305 { rounds, .. } = &mut parsed.kdf_info else {
        panic!("expected PBKDF2")
    };
    *rounds = u32::MAX;
    for encoded in [rmp_serde::to_vec_named(&parsed).unwrap(), serde_json::to_vec(&parsed).unwrap()]
    {
        let before = std::time::Instant::now();
        assert!(matches!(
            StoreCipher::import_with_round_limit("original-passphrase", &encoded, 1_000_000),
            Err(Error::KdfRoundLimit { found: u32::MAX, max: 1_000_000 })
        ));
        assert!(
            before.elapsed() < std::time::Duration::from_secs(5),
            "cost rejection must precede key derivation"
        );
    }
}

#[test]
fn bounded_import_exact_limit_is_inclusive_and_key_mode_is_unchanged() {
    let original = StoreCipher::new().unwrap();
    let encoded = original._insecure_export_fast_for_testing("original-passphrase").unwrap();
    assert!(StoreCipher::import_with_round_limit("original-passphrase", &encoded, 1000).is_ok());
    assert!(matches!(
        StoreCipher::import_with_round_limit("original-passphrase", &encoded, 999),
        Err(Error::KdfRoundLimit { found: 1000, max: 999 })
    ));
    let encoded = original.export_with_key(&[7; 32]).unwrap();
    assert!(matches!(
        StoreCipher::import_with_round_limit("original-passphrase", &encoded, 1_000_000),
        Err(Error::KdfMismatch)
    ));
}

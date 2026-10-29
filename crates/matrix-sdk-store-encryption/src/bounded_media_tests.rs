use super::*;

#[test]
fn bounded_media_msgpack_canonical_limits_authentication_and_legacy_compatibility() {
    let cipher = StoreCipher::new().unwrap();
    for size in [0, 1, 255, 256, 65_535] {
        let plain = vec![255; size];
        let encrypted = cipher.encrypt_value_data(plain.clone()).unwrap();
        let encoded = rmp_serde::to_vec_named(&encrypted).unwrap();
        assert!(
            encoded.len() <= size + 128,
            "canonical encrypted overhead must fit SQLite's pre-read bound"
        );
        let decoded = cipher.decrypt_value_data_bounded_msgpack(&encoded, size).unwrap();
        assert_eq!(decoded, plain);
        assert!(decoded.capacity() <= size + 16);
        assert_eq!(
            cipher.decrypt_value_data(rmp_serde::from_slice(&encoded).unwrap()).unwrap(),
            plain
        );
        if size > 0 {
            assert!(
                matches!(cipher.decrypt_value_data_bounded_msgpack(&encoded, size - 1), Err(Error::Length(limit, actual)) if limit == size - 1 && actual == size)
            );
        }
    }
    let mut encrypted = cipher.encrypt_value_data(vec![1; 16]).unwrap();
    encrypted.ciphertext[0] ^= 1;
    let encoded = rmp_serde::to_vec_named(&encrypted).unwrap();
    assert!(matches!(
        cipher.decrypt_value_data_bounded_msgpack(&encoded, 16),
        Err(Error::Encryption(_))
    ));
}

#[test]
fn bounded_media_msgpack_refuses_sequence_and_truncated_huge_binary_headers() {
    let cipher = StoreCipher::new().unwrap();
    // The serialized map is tiny regardless of its untrusted size hint. The
    // borrowed binary visitor has no sequence/owned-buffer allocation branch.
    let prefix = b"\x83\xa7version\x01\xaaciphertext";
    for header in [
        b"\xdd\xff\xff\xff\xff".as_slice(),
        b"\xc6\xff\xff\xff\xff".as_slice(),
        b"\xdb\xff\xff\xff\xff".as_slice(),
    ] {
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(header);
        assert!(matches!(
            cipher.decrypt_value_data_bounded_msgpack(&bytes, 0),
            Err(Error::Deserialization(_))
        ));
    }
    // Even a complete sequence must not be accepted as a ciphertext binary.
    let encoded = rmp_serde::to_vec_named(
        &serde_json::json!({"version":1,"ciphertext":vec![0;16],"nonce":vec![0;24]}),
    )
    .unwrap();
    assert!(matches!(
        cipher.decrypt_value_data_bounded_msgpack(&encoded, 0),
        Err(Error::Deserialization(_))
    ));
    // Nonce decoding is fixed-size and cannot follow an array's claimed size.
    let mut bytes = prefix.to_vec();
    bytes.extend_from_slice(b"\xc4\x10");
    bytes.extend_from_slice(&[0; 16]);
    bytes.extend_from_slice(b"\xa5nonce\xdd\xff\xff\xff\xff");
    assert!(matches!(
        cipher.decrypt_value_data_bounded_msgpack(&bytes, 0),
        Err(Error::Deserialization(_))
    ));

    // rmp-serde can expose an invalid UTF-8 Str as borrowed bytes. Its marker
    // must not bypass either the allocation bound or AEAD authentication.
    let mut bytes = prefix.to_vec();
    bytes.extend_from_slice(b"\xd9\x11");
    bytes.extend_from_slice(&[255; 17]);
    bytes.extend_from_slice(b"\xa5nonce\xdc\x00\x18");
    bytes.extend_from_slice(&[0; 24]);
    assert!(matches!(
        cipher.decrypt_value_data_bounded_msgpack(&bytes, 0),
        Err(Error::Length(0, 1))
    ));
    assert!(matches!(
        cipher.decrypt_value_data_bounded_msgpack(&bytes, 1),
        Err(Error::Encryption(_))
    ));
}

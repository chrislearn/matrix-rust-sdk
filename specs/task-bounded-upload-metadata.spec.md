spec: task
name: "Bound explicit upload preflight responses for governed native clients"
satisfies: [REQ-CLIENT-QUALITY-REMEDIATION]
tags: [active, rust, native, http, security]
---

## Intent

Repair the R18 upload-preflight response collector without changing default SDK
behavior or the existing governed media POST outcome protocol. This is an
endpoint-specific native opt-in policy, not a general SDK or process memory cap.

## Constraints

### Must
- Enable the private policy only when ClientBuilder has an UploadGovernor and before homeserver discovery can request server versions.
- Limit versions responses to 256 KiB and authenticated or legacy media config plus empty-key m.room.encryption state responses to 64 KiB, including error statuses.
- Select exact typed Ruma endpoints; the generic state endpoint also requires the actual encryption event type and empty state key. Other state events and other endpoints retain their original behavior.
- Check declared Content-Length and count actual decoded chunks before append; reject oversized bodies rather than truncating a valid JSON prefix.
- Preserve serialization errors, existing HTTP fences, retry classification, cached-response behavior and automatic token refresh for accepted responses.
- Preserve the existing 64 KiB governed POST limit and its UploadResponseTooLarge/Unknown semantics; GET failures use separate permanent MetadataResponseTooLarge errors and never invent POST admission.
- Exercise actual production HTTP calls with declared chunked and conflicting length framing, builder discovery, both media config endpoints and encryption lookup; retain default and unrelated endpoint positive controls.

### Must Not
- Do not add a new request_scope, raw upload body owner or token refresh suppression to metadata requests.
- Do not impose a whole-Client response cap or change default non-governed or wasm paths.
- Do not claim earlier R07 binary or trusted-metadata memory measurements qualify this new source.
- Do not edit the frozen upload Governor worktree, run Cargo, push or create a pull request before parent source review.

## Boundaries

### Allowed Changes
- crates/matrix-sdk/src/http_client/mod.rs
- crates/matrix-sdk/src/http_client/native.rs
- crates/matrix-sdk/src/http_client/upload_preflight.rs
- crates/matrix-sdk/src/client/builder/mod.rs
- crates/matrix-sdk/src/error.rs
- crates/matrix-sdk/src/upload_governor/tests.rs
- crates/matrix-sdk/src/upload_governor/tests/metadata.rs
- specs/task-bounded-upload-metadata.spec.md
- knowledge/requirements/req-client-quality-remediation.md
- HAGENCY-BOUNDED-UPLOAD-METADATA.md
- HAGENCY-BOUNDED-UPLOADS.md

## Acceptance Criteria

Scenario: Governed preflight endpoints reject actual oversized HTTP responses
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::metadata
    Level: integration
    Test Double: actual loopback raw HTTP streaming peer and production SDK requests
  Given cold version discovery authenticated or legacy media config and unknown encryption state
  When a declared or actual chunked response exceeds its endpoint budget
  Then collection fails with the distinct permanent metadata error without a media POST false plain-state cache write or upload unknown outcome

Scenario: Accepted authentication and default behavior remain compatible
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::metadata
    Level: integration
    Test Double: actual HTTP status parsing refresh and default client paths
  Given accepted metadata malformed JSON a closed original fence default clients and unrelated endpoints
  When the same production SDK APIs execute
  Then valid results normal 401 refresh serialization and parse failures keep their original semantics while default and unrelated large responses remain accepted

## Decisions

The source starts at f2c042cf1359d974c98dd6a0c72ea61086f5deaa. Inherited Governor
runtime is de44df66601cdeb0f5b2102452f94382c41d093b; this worktree is independent
of the frozen R07 App/SDK trees. No new dependency or lockfile change is needed. The inherited R07 contract and
its recorded measurements describe that earlier source; this Task Contract
adds only the explicit preflight limits and does not reclassify its evidence.

This policy bounds only the listed response bodies. Well-known discovery,
OAuth/Matrix refresh responses, sync and other general SDK endpoints are outside
this change. Native transport/decompression/header overhead and cached data are
not claimed to equal the body budget, and this is not an adversarial whole-SDK
memory qualification. Source review precedes compilation and new test evidence.

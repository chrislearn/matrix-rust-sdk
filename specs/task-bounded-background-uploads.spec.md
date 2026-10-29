spec: task
name: "Bound opt-in native background and restored upload ownership"
tags: [active, rust, native, media, send-queue]
---

## Intent

Provide disabled-by-default native upload admission for applications that share
one Governor across clients. Preserve existing SDK send/outbox/auth behavior and
format while bounding original file-buffer jobs. This is not streaming.

## Constraints

### Must
- Keep default SDK paths behavior-compatible and the original64MiB accepted App source size.
- Limit one active owned operation sixteen accepted metadata jobs including active and128MiB queued cache-write payload; bounded reads cover main thumbnails restore and post-upload thumbnail recache.
- Await original SQLite work to actual return even if observers disappear; move synchronous attachment encryption off Tokio workers and keep actual serialized HTTP Bytes owners charged until release.
- Keep existing client-wide and per-request fences intact; RequestConfig overrides must not erase them or change automatic token refresh.
- Unknown original transport or worker outcomes persist the existing GenericApiError wedge with safe stable messages and retain the original transaction until explicit confirmed unwedge; queue enabling or restart must not resend them.
- Capacity defers and wakes without dropping or wedging durable rows; over-limit or unsupported stores report accurate recoverable paused errors without MissingMediaContent substitution.
- Preserve original pause semantics: queued work not started may defer, already started work may finish its original status; no credential or account protocol redesign.
- Fail closed and retain unknown charged custody on original worker failure instead of pretending IO completion.
- Use actual SQL/HTTP/encrypted upload fixtures alongside explicitly labeled controlled original-store-future cancellation tests.

### Must Not
- Do not alter default APIs persisted queue/error formats auth callbacks or AuthCtx ownership.
- Do not move QueueStorage-lock-dependent outbox commits into a worker awaited by an operation holding that lock.
- Do not claim Rust credits bound native SQLite RSS or entire process memory.
- Do not edit the parent's sdk-bounded-cache checkout or push/open PR before review.

## Boundaries

### Allowed Changes
- crates/matrix-sdk/src/lib.rs
- crates/matrix-sdk/src/client/mod.rs
- crates/matrix-sdk/src/client/builder/mod.rs
- crates/matrix-sdk/src/client/futures.rs
- crates/matrix-sdk/src/http_client/mod.rs
- crates/matrix-sdk/src/http_client/native.rs
- crates/matrix-sdk/src/error.rs
- crates/matrix-sdk/src/upload_governor.rs
- crates/matrix-sdk/src/upload_governor/**
- crates/matrix-sdk/src/send_queue/mod.rs
- crates/matrix-sdk/src/send_queue/upload.rs
- crates/matrix-sdk/src/send_queue/progress.rs
- crates/matrix-sdk/src/send_queue/tests.rs
- crates/matrix-sdk/src/send_queue/tests/**
- crates/matrix-sdk/tests/integration/send_queue.rs
- crates/matrix-sdk/tests/integration/**
- specs/task-bounded-background-uploads.spec.md
- HAGENCY-BOUNDED-UPLOADS.md
- crates/matrix-sdk/CHANGELOG.md

## Acceptance Criteria

Scenario: Original cache crypto and body custody survive observer cancellation
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::custody
    Level: integration
    Test Double: controlled original MediaStore futures actual SQLite and actual serialized HTTP Bytes
  Given accepted jobs held SQL bodies cancellations and worker failures
  When observers disappear or new requests compete
  Then original quotas persist until actual release queue admission remains finite and unknown custody prevents unsafe reuse

Scenario: Restored and new outbox uploads preserve the actual SDK protocol
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::outbox
    Level: integration
    Test Double: actual SDK SQLite restore and loopback HTTP with encrypted content
  Given multiple Rooms main files thumbnails oversized legacy blobs and paused requests
  When governed uploads run retry refresh and finish
  Then accepted data remains correct actual transaction identity and cache formats are preserved and failures retain visible recoverable outbox state

Scenario: Global fences and default behavior remain compatible
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::compatibility
    Level: integration
    Test Double: actual client HTTP admission refresh and queue APIs
  Given default clients shared governed clients request overrides and retirement
  When media and other requests execute
  Then default SDK behavior and client-wide HTTP fences remain while automatic401 refresh is not suppressed by quota ownership

Scenario: Original HTTP result carries its client through durable receipt commit
  Test:
    Package: matrix-sdk
    Filter: send_queue::tests::governed_receipt_and_default_result
    Level: integration
    Test Double: actual HTTP SQLite and controlled idle automatic observer
  Given a real original upload result and no remaining external client owner
  When default or governed handling commits the original durable receipt
  Then the result lease preserves the client until commit succeeds and releases it afterward without another send

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
- Keep default SDK paths behavior-compatible and the original 64 MiB accepted App source size.
- Limit one active owned operation sixteen accepted metadata jobs including active and 128 MiB queued cache-write payload; bounded reads cover main thumbnails restore and post-upload thumbnail recache.
- Bound only original governed upload POST response bodies to 64 KiB with Content-Length preflight and actual incremental chunk accounting; oversized responses after POST admission become durable Unknown. General metadata responses remain a separately recorded R18.
- Await original SQLite work to actual return even if observers disappear; move synchronous attachment encryption off Tokio workers and keep actual serialized HTTP Bytes owners charged until release.
- Keep existing client-wide and per-request fences intact; RequestConfig overrides must not erase them or change automatic token refresh.
- Unknown original transport or worker outcomes persist the existing GenericApiError wedge with safe stable messages and unique non-secret per-failure receipts and retain the original transaction until explicit confirmed unwedge; queue enabling or restart must not resend them.
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
- Cargo.lock
- crates/matrix-sdk/Cargo.toml
- crates/matrix-sdk/src/request_fence.rs
- .github/workflows/hagency-bounded-media.yml
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
  Then default SDK behavior and client-wide HTTP fences remain while automatic 401 refresh is not suppressed by quota ownership

Scenario: Original HTTP result carries its client through durable receipt commit
  Test:
    Package: matrix-sdk
    Filter: send_queue::tests::governed_receipt_and_default_result
    Level: integration
    Test Double: actual HTTP SQLite and controlled idle automatic observer
  Given a real original upload result and no remaining external client owner
  When default or governed handling commits the original durable receipt
  Then the result lease preserves the client until commit succeeds and releases it afterward without another send

Scenario: Original governed POST responses stop at the actual incremental bound
  Test:
    Package: matrix-sdk
    Filter: upload_governor::tests::responses
    Level: integration
    Test Double: actual raw HTTP declared and chunked bodies including a false length header
  Given original media POSTs and large peer response bodies
  When declared length or actual chunks exceed 64 KiB
  Then the original transfer is retained as durable unknown without collecting the full body or automatically retrying

Scenario: Conditional retry rejects replacement failures under the original queue lock
  Test:
    Package: matrix-sdk
    Filter: send_queue::tests::conditional_unknown_retry
    Level: integration
    Test Double: actual SQLite outbox original queue lock external SQLite writer exclusion and controlled native worker failure
  Given a confirmed unknown receipt or an ordinary stale known-failure retry
  When a newer unknown main or thumbnail status is committed or original client authority retires while that operation waits for the original queue lock
  Then every related status is checked before any clear the newer receipt and original transactions remain no HTTP starts and admitted original SQLite guard permit client and quota persist through observer cancellation until real completion

## Decisions

The Governor branch began at 3ccd3851562b1dee920a6d9f3e40bfd77fe3d279 and additionally merges the separately reviewed SDK4 source 95de9a4e393921c446b2b79e5d72ad09411ef586 before final validation. SDK4 source changes are an inherited dependency prerequisite, not this Governor implementation's boundary delta.

R18 remains open: /versions, /media/config and encryption-state preflight metadata responses use the existing general SDK response collector. The upload soak uses trusted bounded metadata responses and cannot qualify an adversarial end-to-end process-memory bound. No global Client response cap or new request_scope is added.

The existing three-OS fatal bounded-media lane also runs the actual Governor,
original result/SQLite receipt, and conditional retry selectors. All prior
media, encryption, read-only identity, WeakClient, history, and strict checks
remain. Local native results do not substitute for pending remote OS checks.

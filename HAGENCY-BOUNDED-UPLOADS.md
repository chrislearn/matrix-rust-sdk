# Opt-in native buffered background uploads

This fork adds `ClientBuilder::upload_governor(Arc<UploadGovernor>)`. The SDK
default remains `None`; a native host opts in and shares the same Arc across all
its clients. Construction is read-only: the single owned OS worker starts with
its first actual job. This is bounded buffering, **not streaming**.

Defaults admit individual main files and thumbnails up to 64MiB, one active
original operation, sixteen accepted jobs including active, and 128MiB total
incoming cache-write bytes. The sixteen-job limit covers Governor transfer
metadata, not existing per-Room SDK tasks, durable outbox metadata, caller-owned
inputs, or persistent cache retention. Hosts must still bound source preparation.

Bounded cache reads cover upload progress lengths, main/thumbnail upload and
post-upload optional thumbnail recaching. Cache writes, original SQLite futures,
synchronous attachment encryption, HTTP sends and actual serialized request Bytes
belong to an owned operation. Dropping its observing future only marks observer
cancellation; it cannot drop already-started original SQL or return quota. Bytes
clones retain the slot and the original Client/credential lease until the last
actual owner releases it. Worker panic retains unknown charged custody and the
Client lease, poisons admission and requires restart; it does not claim IO ended.

Queued Governor closures capture weak client/room references and upgrade only
when original work starts. Existing callers may hold strong clients while
awaiting their work: in particular send_attachment and the QueueStorage completion
critical section. This patch does not redesign those original transactions.
Stopped admission returns those observers, releases the original outbox guard and
leaves a visible recoverable dependent receipt; it does not add a permanent
strong-client capacity-wait cycle.

Transient capacity preserves the durable request and waits for capacity changes
or the original Room notifier, then retries that same outbox transaction. Size,
unsupported-store, paused, stopped and unknown-operation failures retain durable
rows without a false permanent MissingMediaContent wedge and require explicit
resume. Existing outbox cancellation and already-started pause semantics remain.
An incoming cache-write can fail before an outbox row exists; the error does not
claim a pending row was created. Optional duplicate-thumbnail recache remains
best effort and logs a bounded rejection rather than reading unlimited data.

The client-wide HTTP request fence and optional request scope are preserved.
RequestConfig::short_retry does not replace either fence. Quota ownership adds no
request scope and does not suppress the SDK's automatic 401 token refresh. An
actual receipt counts transport admission after both original fences succeed.
Fence closure with zero original media POST admissions is paused before
transport; closure after admission is an unknown outcome and cannot cause
automatic media resubmission. Original event transaction IDs and unknown event
handling are unchanged.

The host must keep its Tokio runtime alive through original-operation shutdown.
Each job captures the runtime at admission; the process worker is not permanently
bound to the first client's runtime. `stop` drains queued observers and prevents
new admission without dropping active IO. `shutdown_until` retains the original
JoinHandle and reports Pending at its deadline or on unknown worker failure;
only Joined confirms actual worker completion. Do not call bounded joins from an
interactive UI handler.

## Memory scope and qualification

A 64MiB limit bounds accepted source bytes, not total memory. SQLite native row
buffers and page cache are additional to Rust vectors. Prior real bundled SQLite
3.53.2 measurements observed roughly 256.16MiB native allocation high-water for
an accepted 64MiB row; that observation is not a portable multiplier or hard RSS
bound. Encryption, Ruma serialization, request clones and HTTP buffering are
also additional to a source vector. No counter proves a heap or RSS bound.

The App release probe uses eight fresh-process workloads: 16/64MiB x 1/20 restored
Rooms x plain/encrypted attachment uploads. Fixture generation uses 64KiB blocks;
whole SDK cache fixture writes occur in a separate setup process. After SDK
warmup, the fixed Rust heap gate is 512MiB, and the exact measured child's
whole-lifetime OS RSS gate is 1GiB. Raw results, source/binary fingerprints and
failing fixtures are retained. Final encrypted event sending and E2EE room-key
exchange are outside this soak window; actual final event transaction and
thumbnail cache formats are covered by separate real SDK regression tests.

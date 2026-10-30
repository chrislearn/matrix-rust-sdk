# Opt-in native buffered background uploads

This fork adds `ClientBuilder::upload_governor(Arc<UploadGovernor>)`. The SDK
default remains `None`; a native host opts in and shares the same Arc across all
its clients. Construction is read-only: the single owned OS worker starts with
its first actual job. This is bounded buffering, **not streaming**.

Defaults admit individual main files and thumbnails up to 64 MiB, one active
original operation, sixteen accepted jobs including active, and 128 MiB total
incoming cache-write bytes. The sixteen-job limit covers Governor transfer
metadata, not existing per-Room SDK tasks, durable outbox metadata, caller-owned
inputs, or persistent cache retention. Hosts must still bound source preparation.

Bounded cache reads cover upload progress lengths, main/thumbnail upload and
post-upload optional thumbnail recaching. Cache writes, original SQLite futures,
synchronous attachment encryption, HTTP sends and actual serialized request Bytes
belong to an owned operation. Dropping its observing future only marks observer
cancellation; it cannot drop already-started original SQL or return quota. Bytes
clones retain the slot and the original Client/credential lease until the last
actual owner releases it. Actual success/error results additionally carry the
original Client lease across the IO-to-outbox handoff until the durable receipt
commit returns; retirement cannot turn an already acquired receipt into a weak
upgrade failure and an unproven resend. Worker panic retains unknown charged custody and the
Client lease, poisons admission and requires restart; it does not claim IO ended.

Queued cache/upload/progress closures capture weak client/room references and
upgrade only when original work starts. The conditional retry is an explicit
exception: after the caller acquires the original queue critical section, its
accepted finite job owns that guard and Client while queued and running. It must
not occupy a Governor job while waiting to acquire the guard. The existing
client-wide fence is checked again before actual status mutation. Existing
callers may also hold strong clients while awaiting their work: in particular
send_attachment and the QueueStorage completion critical section. This patch does not redesign those original transactions.
Stopped admission returns those observers, releases the original outbox guard and
leaves a visible recoverable dependent receipt; it does not add a permanent
strong-client capacity-wait cycle.

Transient capacity preserves the durable request and waits for capacity changes
or the original Room notifier, then retries that same outbox transaction. Size,
unsupported-store, paused and stopped failures retain durable rows without a
false permanent MissingMediaContent wedge and require explicit resume. Unknown
transport or original-worker outcomes instead persist the existing
GenericApiError wedge with a stable safe message: restart/set_enabled alone
cannot resend them. The host uses is_unknown_upload on typed or restored errors
and asks for explicit confirmation before SendHandle::unwedge_if_current_error.
Each new unknown failure has a non-secret per-attempt receipt in the existing
GenericApiError message. Its live and restored errors carry the same receipt.
The conditional API holds the original Room queue store lock, checks all current
main/thumbnail/event statuses before any clear, and refuses a successor unknown
or an already active original request. Ordinary retries pass None and refuse any
current related unknown. After waiting for the original queue lock, the
conditional API also admits its status mutation through the existing client-wide
fence before any write. Retirement while waiting refuses the retry; an admitted
original status write retains that permit until actual completion. In an opted-in
client, the caller acquires the original queue guard before occupying a Governor
job, then transfers that guard/client/permit to the existing owned worker. The
worker never reacquires a QueueStorage lock. Observer cancellation cannot drop
started original status IO; a full queue refuses admission without clearing the
unknown row. Unknown panic retains the actual guard and client with poisoned
custody. Final SQLite owners and queued closures are explicitly released inside
their original runtime context before quota reuse; zero job counters do not mean
all SDK background pool teardown has completed. No new request_scope or authentication policy is added. Legacy SendHandle::unwedge remains unconditional for
compatibility; this is not a cross-process SQLite compare-and-swap guarantee.
The restart/enable guarantee applies after the Unknown status is durably
recorded. A process crash or persistence failure between the HTTP side effect
and its status commit remains an existing uncertainty window; this is not
exactly-once delivery or a new pre-POST intent protocol.
Existing outbox cancellation and already-started pause semantics remain.
An incoming cache-write can fail before an outbox row exists; the error does not
claim a pending row was created. Optional duplicate-thumbnail recache remains
best effort and logs a bounded rejection rather than reading unlimited data.

Original governed media POST responses retain their 64 KiB response body limit:
declared Content-Length is checked before collection and actual chunks are
counted before appending. An oversized POST response yields durable Unknown
because there is no trusted media receipt; it cannot trigger automatic retry.
The independent [preflight response change](HAGENCY-BOUNDED-UPLOAD-METADATA.md)
adds endpoint-specific budgets only for explicitly governed native clients:
versions 256 KiB, media configuration and empty-key encryption state 64 KiB.
Oversized GETs have a distinct permanent metadata error rather than POST Unknown.
No whole Client cap is introduced. Other metadata, well-known and refresh paths
remain outside that change; the original soak's trusted metadata does not prove
an adversarial end-to-end memory bound or qualify this new runtime source.

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

A 64 MiB limit bounds accepted source bytes, not total memory. SQLite native row
buffers and page cache are additional to Rust vectors. Prior real bundled SQLite
3.53.2 measurements observed roughly 256.16 MiB native allocation high-water for
an accepted 64 MiB row; that observation is not a portable multiplier or hard RSS
bound. Encryption, Ruma serialization, request clones and HTTP buffering are
also additional to a source vector. No counter proves a heap or RSS bound.

The App release probe uses eight fresh-process workloads: 16/64 MiB x 1/20 restored
Rooms x plain/encrypted attachment uploads. Fixture generation uses 64 KiB blocks;
whole SDK cache fixture writes occur in a separate setup process. After SDK
warmup, the fixed Rust heap gate is 512 MiB, and the exact measured child's
whole-lifetime OS RSS gate is 1 GiB. Raw results, source/binary fingerprints and
failing fixtures are retained. Final encrypted event sending and E2EE room-key
exchange are outside this soak window; actual final event transaction and
thumbnail cache formats are covered by separate real SDK regression tests.

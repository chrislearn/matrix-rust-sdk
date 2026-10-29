# Bounded reads of existing media cache entries

This additive change starts from Client's exact `919a38b3ae6d0db1f310cb1fcad6b274380f6811`
revision on `codex/mandatory-encrypted-send`. It preserves that branch's existing
mandatory encrypted send, bounded history, storage and authorization changes.

`MediaStore::get_media_content_bounded(request, max_bytes)` returns a real cache
miss only when absent. `Exceeded`, `Unsupported`, `InvalidLimit` and underlying
store/integrity errors remain distinct. Its default does not call the original
unbounded API; third-party and IndexedDB backends remain source-compatible and
explicitly unsupported until they implement a bounded read. The erased store
facade forwards the method. Existing `get_media_content` behavior is unchanged.

Memory checks the original entry's length before cloning under one write lock.
SQLite preflights the actual BLOB length before both the timestamp UPDATE
and content copy. Non-BLOB entries are rejected with a lazy type-checked SQL
expression, without interpreting TEXT character/NUL length or materializing a
cast. The same transaction retains its snapshot; once its write upgrade
succeeds, another connection/process cannot replace the checked entry before
the copy. A conflicting write upgrade returns a backend error. Plain reads copy at most `max_bytes`. Encrypted at-rest entries use the
existing canonical MessagePack `EncryptedValue`, not JSON/base64: ciphertext is
binary and the nonce is fixed at 24 bytes. The encoded blob is limited to
`max_bytes + 128` with checked arithmetic before reading. A borrowed-bytes
visitor rejects sequence/owned-buffer allocation hints, checks the implied plaintext
length, and only then copies/authenticates with in-place AEAD. No failed tag
produces usable plaintext. rmp-serde exposes invalid UTF-8 strings as borrowed
bytes too; the same bound and authentication apply. Actual returned length is
the vector's `len()`.

Encrypted Rust content buffers peak at at most `2 * (max_bytes + 128)`, including
the borrowed input envelope plus the owned ciphertext/plaintext allocation.
This is a Rust content-buffer bound, not a combined SQLite/native heap bound.
SQLite may additionally rebuild an accepted row during its timestamp UPDATE,
using temporary native content buffers; its configured connection/page caches
and allocator bookkeeping are separate too. Their allocation depends on the
accepted entry size and SQLite build. Callers must distinguish Rust scratch
from native SQLite memory when enforcing global memory and concurrency budgets. This API does not promise a process RSS
limit, bound existing stored entries, or stream uploads/downloads over HTTP.

Dropping an async observer cannot stop a SQLite closure already running under
`interact`. Account retirement/cancellation must keep scratch/operation permits
in the original shielded worker until the read actually completes. The SDK
regression pauses that original transaction and proves that a canceled observer
still leaves a second connection's writer blocked until the original IO resumes.

Targeted verification (use an independent target directory):

```sh
cargo test --locked -p matrix-sdk-base --lib bounded_media_
cargo test --locked -p matrix-sdk-sqlite --features bundled --lib media_store::
cargo test --locked -p matrix-sdk-store-encryption --lib
cargo test --locked -p matrix-sdk --features testing,bundled-sqlite --lib public_weak_client
cargo test --locked -p matrix-sdk --features testing,bundled-sqlite,hagency-bounded-history --lib public_weak_client
cargo test --locked -p matrix-sdk --features testing,bundled-sqlite,hagency-bounded-history --lib event_cache::retention::tests
cargo clippy --locked --no-deps -p matrix-sdk-base -p matrix-sdk-sqlite -p matrix-sdk-store-encryption --features matrix-sdk-sqlite/bundled,matrix-sdk-sqlite/crypto-store --lib --tests -- -D warnings
cargo clippy --locked --no-deps -p matrix-sdk --features testing,bundled-sqlite --lib --tests -- -D warnings
cargo clippy --locked --no-deps -p matrix-sdk --features testing,bundled-sqlite,hagency-bounded-history --lib --tests -- -D warnings
```

The dedicated pull-request workflow targets the actual
`codex/mandatory-encrypted-send` base, with fatal Linux, macOS and Windows jobs.
It does not substitute a Client release qualification or account/platform test.
The strict lint uses SQLite's crypto-store feature as well as its default
state/event/media features; three pre-existing shared utility methods have
callers only in crypto-store, so a partial-feature `-D warnings` lint otherwise
reports their dead code. No lint exception or source suppression is added.

The first hosted run at `758d0a95fc5dcf33b2da33e673a6a7b50f17b819`
passed the new Linux and Windows checks, workflow lint and full documentation.
Its original linked-chunk benchmark built successfully, but the pinned
CodSpeed runner 5.2.1 failed at launch with `Unsupported system` on the newly
migrated `ubuntu-latest` image (26.04.1). The benchmark workflow now explicitly
uses Ubuntu 24.04 and retains all six original benchmarks without error
exemptions. The reviewed toolchain action commit is unchanged; its moving-tag
comment is replaced with an accurate immutable-revision description.

At `689a19cdbc498b10231a04acd001a44d36f919fa`, all three bounded
platform jobs and documentation/lint passed. Ubuntu 24.04 also completed actual
linked-chunk simulation measurements, then CodSpeed's cloud upload returned 401
(`Repository not found or the user does not have access to it`). That original
failure remains evidence, not a successful cloud integration. No account,
credential or external project was provisioned.

For `chrislearn/matrix-rust-sdk` only, the workflow now uses the official pinned
runner v5.2.1 `CODSPEED_SKIP_UPLOAD` flag. Its exact source tag resolves to
`8868af5a1e582ade8cbe080ae6f79da12650d131`: `src/cli/shared.rs`
declares the flag, and `src/executor/orchestrator.rs` checks it only after every
executor has run, immediately before uploading. All six suites still compile
and measure with `allow-empty: false`. The pinned simulation executor does not
actually enforce that flag locally (its old validation happens on upload), so
fork CI additionally runs `tools/check_benchmark_profiles.py` before accepting
results. Each suite must contain matching named Callgrind parts, the pinned Rust
integration metadata, an Ir event, actual nonzero instruction cost rows and
nonzero final totals in the same completed part. Metadata, termination wrappers,
logs, empty or truncated parts cannot pass. Actual combined profiles have zero
`summary` headers; the checker deliberately uses their completed `totals`.
Six Python regression groups cover those failure modes, and the checker was
also run on four real hosted artifacts (crypto 6, linked-chunk 21, store 3,
timeline 1 measured parts).
Each job must upload the actual profile
files/logs as a seven-day GitHub artifact; missing output fails the job. The
official `create_profile_folder` writes `profile.*.out` under `TMPDIR`, which is
set to a dedicated runner temporary folder. The upstream `matrix-org` workflow
retains cloud upload. Successful fork CI proves offline execution/measurement
and retained results, not CodSpeed cloud publication or comparison baselines.

Idle native file-dialog callbacks must not retain retired SDK stores or
background resources. The same change exposes the existing opaque `WeakClient`
at the crate root and `Client::downgrade`, forwarding the original
`Weak<ClientInner>` implementation. `from_inner` and `strong_count` stay crate
private; `from_client` and `get` keep their original behavior. A real unlogged
Client test verifies that weak clones add no strong owner, upgrades retain the
actual original inner identity, an existing Client clone remains usable, and
all weak handles immediately return `None` after the last strong owner drops.
This lets idle callbacks retain only weak state. Active network/IO jobs must
still retain strong custody until their actual completion, and every upgraded
handle still needs the caller's independent account/session authorization fence.
The public Weak Client regression and Client library/test strict lint cover both
default and App `hagency-bounded-history` profiles. Initial default-profile
Clippy exposed a pre-existing cfg-only question-mark lint: when bounded cleanup
was compiled out, its error branch only returned the error. An equivalent cfg
split keeps the original bounded cleanup block unchanged and calls `start_from`
exactly once in either profile. No cleanup/cancellation behavior or lint
suppression was changed; the original failed Clippy log remains retained.

The bounded-profile test lint also exposed redundant `&store` borrows after
`as_clean()` already returned a store reference. Only those test-call borrows
were removed; every fixture, production path and behavioral assertion remains.

Native-allocation verification found and fixed an important preflight ordering
problem: the old timestamp UPDATE ran before the length check. In a fresh
macOS arm64 process linked to the actual bundled SQLite 3.53.2, an existing 128MiB BLOB
caused an additional 268,763,136 bytes of peak process RSS during that UPDATE,
even with a 64KiB page cache and mmap disabled. The type/length preflight now
runs first. An isolated actual SDK regression uses the real SQLite allocator
high-water counter, reset after fixture construction; 128MiB oversized BLOBs
and invalid NUL-containing TEXT entries showed zero additional native bytes
in both plain and encrypted-store modes. A rejecting UPDATE trigger separately
proves those failures precede row mutation, and is removed before measuring
native allocation so it cannot mask the old allocating path.

The same real SDK probe successfully returned/authenticated accepted 64MiB
content in both modes. With those local SQLite/cache settings its additional
peak native allocation was 268,599,296 bytes in each mode, beyond Rust content
buffers. This is an observed local result, not a portable multiplier, total
heap budget or RSS guarantee. All 32 actual SQLite media tests, including
independent-connection replacement and observer cancellation, passed after
this correction; the strict affected-library/test Clippy checks also passed.

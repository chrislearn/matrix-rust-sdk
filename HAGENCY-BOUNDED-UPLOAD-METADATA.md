# Opt-in native upload-preflight response budgets

This source change starts at `f2c042cf1359d974c98dd6a0c72ea61086f5deaa`, separately
from the frozen R07 upload branch. Parent source review and independent review
preceded a fresh build: all 17 production HTTP metadata regressions passed,
followed by all 44 combined Governor/receipt/conditional-retry regressions.
Bounded-history and default own lib/tests strict Clippy passed (73.0 and
43.14 seconds). The unmodified lifecycle passes 3/3, including its exact eleven-path boundary
and two real 17-case groups. Current-head platform CI remains pending. Neither R07's old executable nor its
trusted-metadata heap/RSS results qualify this new runtime's memory.

A native `ClientBuilder` with an explicit `UploadGovernor` enables a private
HTTP policy **before** homeserver discovery. This covers the raw discovery
version request, authenticated/failsafe version fetches and version requests
made while selecting a request path, without adding a request scope or media
body owner to a metadata GET.

| Exact endpoint | Decoded response-body budget |
| --- | --- |
| `GET /_matrix/client/versions` | 256 KiB |
| Authenticated or legacy media configuration | 64 KiB |
| Empty-state-key `m.room.encryption` lookup | 64 KiB |

The selector compares Ruma's typed endpoint path templates, with an additional
actual event-type/empty-key check for the generic state API. Reverse-proxy base
paths and percent-encoded Room IDs do not broaden this to arbitrary state
content. Other endpoints, other state types and nonempty state keys keep the
original collector. Clients without a Governor and wasm keep their default
behavior. No new dependency or persisted format is introduced.

One shared incremental collector handles these requests and existing governed
POST responses. It checks declared length, counts every decoded chunk before
append, and rejects excess data rather than accepting a truncated JSON prefix.
This applies to successful and error statuses alike. Under-limit data still
passes through the same Ruma response/error deserialization. Existing HTTP
fences, retry parameters, original token refresh and serialization error paths
are retained.

An over-limit GET produces the distinct, permanent
`HttpError::MetadataResponseTooLarge` with a static endpoint label. It does not
produce an upload admission receipt or `OutcomeUnknown`. The existing governed
media POST remains limited to 64 KiB and retains its original
`UploadResponseTooLarge` and durable Unknown semantics. An oversized encryption
404 is not accepted as evidence that a Room is plaintext; no Room state is
updated after that error.

The new `upload_governor::tests::metadata` selector calls production SDK APIs
against a real raw loopback peer. Large responses are generated in 64 KiB
blocks, including declared, chunked and conflicting small Content-Length cases.
Fixtures cover pre-Client discovery, cold versions, both media config routes,
encryption lookup/error status, exact and one-byte-over boundaries, decoded gzip
versus wire length, malformed accepted JSON,
missing authentication, actual 401 refresh, closed original fences, retained
outbox/cache data and zero media POST after rejection. Large default-client and
unrelated-state positive controls ensure the policy is opt-in and narrow.
The parent fresh 17-case run passed in 12.12 seconds. The separate combined
44-case run passed in 12.24 seconds and includes the original POST, actual
SQLite custody and conditional-retry regressions. These local results do not
substitute for current-head CI or final composed App memory measurements.

The body budget is not a total-process-memory limit. Transport, decompression,
headers, parser overhead and existing caches remain separate. Well-known,
OAuth/Matrix refresh, sync and other general metadata responses are outside
this patch; no adversarial whole-SDK bound is claimed. A new source/binary and
fresh memory evidence are needed before attaching measurements to this runtime.


The combined Governor contract also passes 7/7 against all 28 candidate paths;
its original admission, custody, POST and retry criteria remain separate from
the new metadata contract. The default send-queue integration selector passes
all 45 cases (4.05 seconds). An earlier narrower `send_queue::` selector ran
42 cases; that record is retained and is not promoted to 45. The only panics in
the 44-case run are the two intentional original-worker fault injections.
The frozen Rust sources match the first source review; later changes are scoped
contract composition and qualification documentation. Fresh composed App memory
and every current-head hosted platform check remain required before merging.

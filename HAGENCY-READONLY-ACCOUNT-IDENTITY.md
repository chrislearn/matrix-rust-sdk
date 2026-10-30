# Existing crypto account identity without store initialization

This additive task starts at merged commit
`a34339e2926c703736e8d66e01cdf9aa933ff4d2` on the actual fork branch
`codex/mandatory-encrypted-send`. It adds only a synchronous original-account
inspector, its public identity/error types, direct regressions, and one fatal
selector in the existing three-platform SDK lane, plus a limited cipher import prerequisite. No dependencies or normal
store lifecycle methods change.

`SqliteCryptoStore::inspect_existing_account_identity(directory, passphrase)`
opens the fixed existing crypto file with SQLite READ_ONLY and NOFOLLOW, without
CREATE, URI, or immutable flags. A single deferred read transaction captures the
current schema, original cipher and account together, including committed live
WAL contents. Only schema 19 is accepted. The original StoreCipher import and
Account unpickler are reused; nothing creates or replaces a cipher/account. The
returned value contains only user/device IDs. Account/cipher decoding errors do
not include their contents. Missing files/accounts, unsupported schemas, corrupt
data and wrong secrets are errors, never freshness evidence.

An earlier proposed “no sidecar writes” boundary was too strong: normal SQLite
WAL readers may need SHM coordination. Immutable mode would silently ignore a
valid live WAL, so it is deliberately not used. The accepted boundary is no
initialization, migration, checkpoint, or writes to the original main database,
account or crypto history. SQLite may update its necessary WAL/SHM reader
coordination; sidecar bytes are not promised unchanged. If ordinary SQLite
read-only access cannot work, inspection fails. SQLite NOFOLLOW is requested and
final symlinks are rejected, but VFS/platform support is not a replacement for
the caller's original path/file-identity and directory-authority checks.

The function performs blocking IO and passphrase derivation and spawns no work.
Async users must call it from an owned worker, retaining the original file,
secret, job capacity and completion until return even when a waiting observer
is dropped. It is an identity observation, not a login or activation receipt.
The Client consumer will use its existing credential worker and grant fences;
no detached Tokio registry, model invocation, or native credential access is
part of this SDK API.

Validation (external target directory, existing locked dependencies):

```sh
HAGENCY_NATIVE_CREDENTIAL_IO=deny cargo test --locked -p matrix-sdk-sqlite --features bundled,crypto-store --lib inspect_existing_account_identity
HAGENCY_NATIVE_CREDENTIAL_IO=deny cargo clippy --locked --no-deps -p matrix-sdk-sqlite --features bundled,crypto-store --lib --tests -- -D warnings
```

The real SQLite fixtures cover current plaintext and passphrase stores with
byte-exact main-file, schema and raw-row preservation (including a history
sentinel); missing-path zero creation; older/newer schema rejection without
migration; wrong/missing keys and corrupt/missing accounts; live uncheckpointed
WAL identity and a concurrent account/cipher replacement retaining one coherent
read snapshot; and dropping an observer while the original owned thread still
retains its file/secret until actual completion. Unix also tests a final-file
symlink rejection. These host fixtures do not qualify mobile storage or OS
credential providers. Current-head platform CI remains required.

The inspector limits PBKDF2 import to 1,000,000 rounds, versus the original SDK
export default of 200,000. `StoreCipher::import_with_round_limit` shares the
original MessagePack/legacy JSON parser and importer, checking the declared
cost before derivation; the original unbounded import retains its behavior.
Excessive costs return a numeric, secret-free KdfRoundLimit error. Standard
exports and original keys are not rewritten or weakened.

Size preflights bound the original BLOB copies (cipher 64 KiB, account roughly
4 MiB) and authenticated account plaintext. They do not prove a complete memory
bound: the reused MessagePack account decoder defaults to depth 1024, and serde
collection hints use cautious initial allocations, but nested/aggregate decoder
allocations and SQLite native memory are not fully isolated by that byte limit.
No complete deserializer/CPU/OS isolation claim is made. The explicit PBKDF2
policy closes the declared-cost amplification before authentication.

The active contract is `specs/task-readonly-account-identity.spec.md`. Its
accepted requirement is a provenance-marked synchronized copy from the Client.
The unmodified agent-spec 1.4 full lifecycle runs with
`HAGENCY_NATIVE_CREDENTIAL_IO=deny`. Its TestVerifier only runs
`cargo test -q -p PACKAGE FILTER` and cannot enable Cargo features. Therefore
its SQLite reader group honestly reports zero matches/Skip under the original
default features; the cipher group and boundaries are checked normally. This
Skip is not a pass. The separately recorded exact feature-enabled SQLite test
command above, strict lint, and current three-platform fatal CI provide the
actual reader evidence. No launcher or default-feature modification hides the
tool limitation.

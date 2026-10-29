spec: task
name: "Inspect original SQLite crypto account identity without initialization"
satisfies: [REQ-CLIENT-QUALITY-REMEDIATION]
tags: [active, rust, security, persistence]
---

## Intent

Allow the credential migration worker to authenticate the original database's
user and device before activating a new grant, without opening a mutable SDK
store, migrating it, or substituting a new key or database.

## Constraints

### Must
- Open only the fixed existing crypto database through SQLite READONLY with no CREATE or immutable flags and one consistent read transaction.
- Read committed live WAL state with SQLite's own coordination; preserve the main database, schema, original account and crypto history.
- Require current schema and original cipher or plaintext mode explicitly; fail on missing original files, accounts, keys, unsupported versions and corrupt values.
- Return only public user and device identity and sanitize cipher or account decode errors.
- Reject stored PBKDF2 cost above one million iterations before key derivation while preserving standard MessagePack and legacy JSON original keys.
- Keep all synchronous inspection inside the caller's owned blocking job until actual completion; observer cancellation does not cancel that original.

### Must Not
- Do not initialize, migrate, checkpoint, replace cipher or account, or treat an error as a fresh database.
- Do not use immutable mode to ignore an effective WAL or claim SQLite sidecar bytes remain unchanged.
- Do not claim encoded byte limits are complete SQLite or deserializer memory bounds, or that identity observation grants login authority.
- Do not access native credential providers, edit Client pins, or alter unrelated SDK history or governance paths.

## Boundaries

### Allowed Changes
- crates/matrix-sdk-sqlite/src/crypto_store.rs
- crates/matrix-sdk-sqlite/src/lib.rs
- crates/matrix-sdk-sqlite/src/crypto_store/inspect.rs
- crates/matrix-sdk-sqlite/src/crypto_store/inspect/tests.rs
- crates/matrix-sdk-store-encryption/src/lib.rs
- crates/matrix-sdk-store-encryption/src/bounded_import_tests.rs
- .github/workflows/hagency-bounded-media.yml
- HAGENCY-READONLY-ACCOUNT-IDENTITY.md
- specs/task-readonly-account-identity.spec.md
- knowledge/requirements/req-client-quality-remediation.md

## Acceptance Criteria

Scenario: Existing original account inspection is coherent and noninitializing
  Test:
    Package: matrix-sdk-sqlite
    Filter: inspect_existing_account_identity
    Level: integration
    Test Double: actual isolated SQLite stores and controlled owned thread observer
  Given original plaintext and encrypted stores live WAL updates missing or damaged originals and unsupported schemas
  When the production read-only inspector reads a transaction or its observer disappears
  Then it returns the coherent original public identity or an explicit error without initialization migration checkpoint main data writes or abandoned original work

Scenario: Original cipher compatibility has a finite passphrase import cost
  Test:
    Package: matrix-sdk-store-encryption
    Filter: bounded_import_
    Level: unit
    Test Double: real original cipher exports in MessagePack and legacy JSON
  Given standard original cipher exports and malicious maximum iteration declarations
  When the bounded import applies the explicit iteration policy
  Then compatible original keys are preserved and excessive costs are rejected before derivation without changing the existing import API

## Out of Scope

Native OS credentials, Client grant/retirement integration, activation, mobile
qualification and arbitrary historical schema migration remain outside this SDK
prerequisite. SQLite's necessary SHM reader coordination is permitted. NOFOLLOW
is requested but platform or VFS support does not replace caller directory and
physical file identity guards. No new dependencies are introduced.

The installed agent-spec 1.4 Cargo TestVerifier cannot select Cargo features.
Its original full lifecycle therefore records the SQLite group as zero-match
Skip under default features, not as passing. Independently run the exact
`cargo test --locked -p matrix-sdk-sqlite --features bundled,crypto-store --lib inspect_existing_account_identity`
selector and strict lint; the existing three-platform SDK lane runs that exact
feature profile fatally. The default cipher group remains executable through
the unmodified lifecycle. No default feature or launcher workaround is used.

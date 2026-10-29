# Weak Client allocation identity

This additive task starts at merged commit
`709ad8ecca8e1413c40e5f1591fcc58c8314eedc` on
`codex/mandatory-encrypted-send`. It exposes only
`WeakClient::ptr_eq(&self, &Self)`, delegating directly to
`std::sync::Weak::ptr_eq` on the existing private `ClientInner` weak handle.
It does not upgrade a handle, read session/store data, perform IO or authorize
an account. It works before login and after the last strong client is dropped.

The real authless client regression uses a shared MemoryStore and identical
configuration to prove that separate ClientInner allocations stay distinct,
client/weak clones compare equal, and comparison does not retain either client.
No network requests, provider calls or native credentials are involved.

Validation, using the locked dependencies and an external target directory:

```sh
cargo test --locked -p matrix-sdk --features testing,bundled-sqlite --lib public_weak_client
cargo test --locked -p matrix-sdk --features testing,bundled-sqlite,hagency-bounded-history --lib public_weak_client
cargo clippy --locked --no-deps -p matrix-sdk --features testing,bundled-sqlite --lib --tests -- -D warnings
cargo clippy --locked --no-deps -p matrix-sdk --features testing,bundled-sqlite,hagency-bounded-history --lib --tests -- -D warnings
```

The existing fatal three-platform `hagency-bounded-media.yml` runs this selector
and both strict profiles for pull requests against the actual branch above.
Existing media/encryption/retention checks and six benchmark suites stay intact.

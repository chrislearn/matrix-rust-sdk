# Bounded disposable history caches

The `chris/bounded-chat-history` branch starts at the exact SDK 0.19.1 published
source commit `b18166c68bb958a21f0bca8b2d8320cb53583362`.

The opt-in `matrix-sdk/hagency-bounded-history` feature bounds disposable live
room/thread event working sets to 512 events and 8 MiB of raw JSON, compacting
to the newest 128 events. A single raw event is limited to 256 KiB. Context
pagination rejects additions exceeding its budget so the application can rebase
around its visible event. Clear plus append resets decoded SDK UI metadata;
local echoes remain under the upstream UI controller's management.

Cold loading reads one last chunk and resets disposable linked-chunk metadata.
SQLite pruning preserves events referenced by any room/thread/pin chunk and
keeps at most 64 detached events per room. Authentication, crypto, durable send
queues, state and server history are outside this disposable cache.

The room cache admits 32 rooms, and each room admits 32 thread and 32 event
contexts; unused caches are reclaimed and all-active saturation returns an
explicit error. Applications should use smaller retained timeline budgets.

These are history working-set budgets. They do not promise a process RSS/GPU
cap or bound HTTP responses, membership/state, crypto sessions, queued sends,
or allocator overhead. An application-owned encrypted history search index is
required for persistent search across discarded working windows.

Validation is performed by Hagency's real SDK/local HTTP/SQLite integration
tests and encrypted storage tests. See its dev-vibe report for measured scope.

Latest-event previews acquire a room cache only for each update. Their old
OnceCell otherwise pins every joined room and prevents safe global eviction.

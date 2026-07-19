# src

The messaging library (see ARCHITECTURE.md for the full map): envelope
(`envelope.rs`), subject taxonomy (`subjects.rs`), pure outbox/inbox core
(`outbox.rs`), Postgres side (`db.rs`, `inbox.rs`), publisher seam
(`publisher.rs`), errors (`error.rs`), compat lineage (`compat_envelope.rs`,
`transactional.rs`), and the relay binary (`main.rs`, `bin/`). Operator-facing
failures (dead-lettered rows, batch errors) log via `tracing`.

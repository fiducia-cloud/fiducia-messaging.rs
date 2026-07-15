# bin

`fiducia-messaging-compat.rs` — the legacy drain loop over
`message_outbox_compat` (core NATS, 250ms interval), built only with
`--features compat-service`. Deployed only where old-format producers remain.

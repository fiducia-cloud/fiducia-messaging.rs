# docs

Engineering notes for fiducia-messaging.rs. The repo-root `README.md` is the
overview and `ARCHITECTURE.md` the module map; deeper follow-up material lives
here.

- `hardening-followups.md` — open risks and decisions after the audit + SeaORM
  migration. Each item is **Open** (needs a fix) or **Deployment** (correct in
  code, but the deployment must do its part). Start here for the compat-service
  transaction-across-NATS-I/O hazard.

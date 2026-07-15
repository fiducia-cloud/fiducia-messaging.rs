# Source layout

Standard envelopes and subjects live beside the transport-agnostic publisher,
outbox/inbox, transactional helpers, and optional database/NATS adapters.
`bin/` contains migration compatibility tooling rather than a broker.

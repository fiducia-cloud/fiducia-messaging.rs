# Workflows

- `ci.yml` hard-gates formatting, all-features warnings-as-errors Clippy,
  locked tests, dependency advisories, and the CLI flag contract on Rust
  `1.95.0`.
- `cli-flags.yml` re-audits the parser-backed flag schema whenever its inputs
  change.

GitHub Actions and audit tooling are immutable commit/version pins. Dependabot
tracks the Rust lockfile, actions, and Docker base image.

## Security baseline

Every executable workflow uses explicit least-privilege permissions, immutable
third-party action or container references, non-persisted checkout credentials,
concurrency control, and a job timeout. The main CI workflow validates this
directory with the digest-pinned actionlint container. Environment mutation is
forbidden unless this README documents a repository-specific platform exception.

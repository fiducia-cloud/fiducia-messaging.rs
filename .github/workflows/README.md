# Workflows

- `ci.yml` hard-gates formatting, all-features warnings-as-errors Clippy,
  locked tests, dependency advisories, and the CLI flag contract on Rust
  `1.95.0`.
- `cli-flags.yml` re-audits the parser-backed flag schema whenever its inputs
  change.

GitHub Actions and audit tooling are immutable commit/version pins. Dependabot
tracks the Rust lockfile, actions, and Docker base image.

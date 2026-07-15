# .github

GitHub Actions for `fiducia-messaging.rs` — CI (fmt, clippy `-D warnings`, locked tests,
`cargo audit`) plus the repo's deploy/docker/flags workflows where present.
Workflow actions are pinned to full commit SHAs per the fleet's
reproducible-build policy (audited by the monorepo's `audit-repo-state.sh`).

# scripts

Helper scripts for working with the crate.

- `with-flags2env.sh` — bridges CLI flags to the environment variables the
  `fiducia-relay` and compatibility binaries read. It runs the pinned
  `flags2env` parser against `.cli-flags.toml`, exports the resulting env map,
  then execs the requested command.

# .nix

Reproducible development environment defined as a Nix flake.

- `flake.nix` — a dev shell with the Rust toolchain (rustc, cargo, rustfmt,
  clippy, rust-analyzer) plus supporting tools (git, direnv, just, bacon, node,
  pnpm, pkg-config, openssl) for all common Linux/macOS systems.
- `flake.lock` — pins the exact nixpkgs revision so the shell is reproducible.

Entered automatically via direnv (`.envrc`) or manually through the `./shell`
helper at the repo root.

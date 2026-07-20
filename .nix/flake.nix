# Nix flake defining the Fiducia dev shell (Rust toolchain + supporting tooling),
# used via direnv (.envrc) and the ./shell helper.
{
  description = "Fiducia development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              rustc
              cargo
              rustfmt
              clippy
              rust-analyzer

              git
              direnv
              just
              bacon

              nodejs
              pnpm

              pkg-config
              openssl
            ];

            shellHook = ''
              echo "Fiducia dev shell (${system})"
            '';
          };
        });
    };
}

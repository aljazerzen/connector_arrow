{
  description = "PRQL development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        essentials = with pkgs; [
          # compiler requirements
          rustup
          clang

          # tools
          cargo-nextest
          bacon
          cargo-audit
          cargo-insta
          cargo-release
          pkg-config
          openssl
          #cargo-llvm-cov

          # actions
          just
          #sd
          #ripgrep
          #nodePackages.prettier
          #nodePackages.prettier-plugin-go-template
          #nixpkgs-fmt
          #rsync
        ];

        dbs = with pkgs; [
          postgresql_15
          sqlite
          mysql
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = essentials ++ dbs;
        };
      });
}

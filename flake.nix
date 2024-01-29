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
          rustup
          clang

          # tools
          just
          cargo-nextest
          cargo-audit
          cargo-release
          pkg-config
          openssl
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

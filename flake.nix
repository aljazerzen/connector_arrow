{
  description = "PRQL development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        fenix_pkgs = fenix.packages.${system};

        dontCheckPython = drv: drv.overridePythonAttrs (old: { doCheck = false; });

        essentials = with pkgs; [
          fenix_pkgs.stable.cargo
          fenix_pkgs.stable.clippy
          fenix_pkgs.stable.rust-src
          fenix_pkgs.stable.rustc
          fenix_pkgs.stable.rustfmt
          fenix_pkgs.stable.rust-analyzer
          clang

          # tools
          just
          cargo-nextest
          cargo-insta
          cargo-audit
          pkg-config
          docker-compose 
          (dontCheckPython python311Packages.yamlfix)
          comrak
          fd
        ];

        dbs = with pkgs; [
          postgresql_15
          sqlite
          mysql
          duckdb
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = essentials ++ dbs;
        };
      });
}

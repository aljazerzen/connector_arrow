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

        dontCheckPython = drv: drv.overridePythonAttrs (old: { doCheck = false; });

        essentials = with pkgs; [
          rustup
          clang

          # tools
          just
          cargo-nextest
          cargo-insta
          cargo-audit
          cargo-release
          pkg-config
          docker-compose 
          (dontCheckPython python311Packages.yamlfix)
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

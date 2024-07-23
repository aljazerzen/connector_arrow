{
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
          (fenix_pkgs.stable.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rustc"
            "rustfmt"
            "rust-analyzer"
          ])
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
          sqlite
          duckdb # version of duckdb here should match to version specified in Cargo.toml
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = essentials ++ dbs;

          # linking duckdb needs this
          LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
        };
      });
}

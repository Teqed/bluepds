
{
  description = "Alternative Bluesky PDS implementation";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };
  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
    let
      buildInputs = with pkgs; [
          openssl
          gcc
          bacon
          sqlite
          rust-analyzer
          rustfmt
          clippy
          git
          nixd
          direnv
          libpq
        ];
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs {
        inherit system overlays;
      };
      rust = pkgs.rust-bin.nightly."2025-03-22".default.override {
        extensions = [
          "rust-src" # for rust-analyzer
          "rust-analyzer"
        ];
        # targets = [ "wasm32-unknown-unknown" ];
      };
      nativeBuildInputs = with pkgs; [ rust pkg-config ];
    in
    with pkgs;
    {
      devShells.default = mkShell {
        inherit buildInputs nativeBuildInputs;
        LD_LIBRARY_PATH = nixpkgs.legacyPackages.x86_64-linux.lib.makeLibraryPath buildInputs;
        RUST_BACKTRACE = 1;
        DATABASE_URL = "sqlite://data/sqlite.db";
      };
    });
}

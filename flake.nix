{
  description = "Alternative Bluesky PDS implementation";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs = { self, nixpkgs, crane, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain (p: p.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
          extensions = [
            "rust-src" # for rust-analyzer
            "rust-analyzer"
          ];
        }));
        
        inherit (pkgs) lib;
        unfilteredRoot = ./.; # The original, unfiltered source
        src = lib.fileset.toSource {
          root = unfilteredRoot;
          fileset = lib.fileset.unions [
            # Default files from crane (Rust and cargo files)
            (craneLib.fileset.commonCargoSources unfilteredRoot)
            # Include all the .sql migrations as well
            ./migrations
          ];
        };
        # Common arguments can be set here to avoid repeating them later
        commonArgs = {
          inherit src;
          strictDeps = true;
          nativeBuildInputs = with pkgs; [
            pkg-config
            gcc
          ];
          buildInputs = [
            # Add additional build inputs here
            pkgs.openssl
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            # Additional darwin specific inputs can be set here
            pkgs.libiconv
            pkgs.darwin.apple_sdk.frameworks.Security
          ];

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
        };

        # Build *just* the cargo dependencies, so we can reuse
        # all of that work (e.g. via cachix) when running in CI
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Build the actual crate itself, reusing the dependency
        # artifacts from above.
        bluepds = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          nativeBuildInputs = (commonArgs.nativeBuildInputs or [ ]) ++ [
            pkgs.sqlx-cli
          ];
          preBuild = ''
            export DATABASE_URL=sqlite:./sqlite.db
            cargo sqlx database create
            cargo sqlx migrate run
          '';
          postInstall = ''
            mkdir -p $out/var/lib/bluepds
            cp ./default.toml $out/var/lib/bluepds/
            cp ./sqlite.db $out/var/lib/bluepds/
          '';
        });
      in
      {
        checks = {
          # Build the crate as part of `nix flake check` for convenience
          inherit bluepds;
        };

        packages = {
          default = bluepds;
          inherit bluepds;
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${system};

          # Additional dev-shell environment variables can be set directly
          # MY_CUSTOM_DEVELOPMENT_VAR = "something else";
          RUST_BACKTRACE = 1;
          NIXOS_OZONE_WL=1;

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = with pkgs; [
            sqlx-cli
            bacon
            sqlite
            rust-analyzer
            rustfmt
            clippy
            git
            nixd
            direnv
          ];
        };

        nixosModules = {
          default = { pkgs, lib, config, ... }: with lib; let
              cfg = config.services.bluepds;
            in
            {
              options.services.bluepds = {
                enable = mkEnableOption "Enable bluepds";
                host_name = mkOption {
                  type = types.str;
                  default = "pds.example.com";
                  description = "The public hostname of the PDS.";
                };
                listen_address = mkOption {
                  type = types.str;
                  default = "0.0.0.0:8000";
                  description = "The address to listen to for incoming requests.";
                };
                test = mkOption {
                  type = types.str;
                  default = "true";
                  description = "Test mode. This instructs BluePDS not to federate with the rest of the AT network.";
                };
                package = mkOption {
                  type = types.package;
                  default = self.packages.${pkgs.system}.default;
                  description = "The path to the bluepds package.";
                };
              };
              config = mkIf cfg.enable {
                systemd.services.bluepds = {
                  description = "ATProto PDS server";
                  wantedBy = [ "multi-user.target" ];
                  after = [ "network.target" ];
                  requires = [ "network-online.target" ];
                  environment = {
                    BLUEPDS_TEST = "${cfg.test}";
                    BLUEPDS_HOST_NAME = "${cfg.host_name}";
                    BLUEPDS_LISTEN_ADDRESS = "${cfg.listen_address}";
                  };
                  serviceConfig = {
                    ExecStart = "${cfg.package}/bin/bluepds";
                    ProtectHome = true;
                    WorkingDirectory= "/var/lib/bluepds";
                    Restart = "on-failure";
                    Type = "exec";
                  };
                };
              };
            };
        };
      });
}
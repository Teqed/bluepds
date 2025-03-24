{
  description = "Alternative Bluesky PDS implementation";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };
  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      pkgsFor = nixpkgs.legacyPackages;
    in {
      packages = forAllSystems (system: {
        default = pkgsFor.${system}.callPackage ./. { };
      });

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
    };
}
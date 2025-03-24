{ pkgs ? import <nixpkgs> { } }:
pkgs.rustPlatform.buildRustPackage rec {
  pname = "bluepds";
  version = "0.0.0";
  cargoLock.lockFile = ./Cargo.lock;
  src = pkgs.lib.cleanSource ./.;
  nativeBuildInputs = with pkgs; [ pkg-config ];
  buildInputs = with pkgs; [ openssl ];
  postInstall = ''
    mkdir -p /var/lib/bluepds
    cp ./default.toml /var/lib/bluepds/
  '';
}

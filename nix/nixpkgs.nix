# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem, ... }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    grpc = pkgs.grpc.overrideAttrs (oldAttrs: {
      version = "1.23.1";
      src = pkgs.fetchFromGitHub {
        owner = "grpc";
        repo = "grpc";
        rev = "v1.23.1";
        sha256 = "1jcyd9jy7kz5zfch25s4inwlivb1y1w52fzfjy5ra5vcnp3hmqyr";
        fetchSubmodules = true;
      };
      # Upstream nixpkgs applies patches that are incompatbile with our version
      # of grpc. So, we disable them.
      patches = [];
    });
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_9_6}/bin:$out/bin
      '';
    });
  };

  nixpkgs = import src {
    inherit system;

    # pin the overlays
    overlays = [overrides];

    config.allowUnfree = true;
    config.allowBroken = true;
  };
in
  nixpkgs

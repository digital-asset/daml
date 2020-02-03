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
      patches = [];
    });
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_9_6}/bin:$out/bin
      '';
    });
    haskellPackages = pkgs.haskellPackages.override {
      overrides = self: super: {
        hlint = super.callPackage ./overrides/hlint-2.1.15.nix {};
        haskell-src-exts = super.callPackage ./overrides/haskell-src-exts-1.21.0.nix {};
        haskell-src-meta = super.callPackage ./overrides/haskell-src-meta-0.8.2.nix {};
      };
    };
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

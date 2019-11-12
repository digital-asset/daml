# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem, ... }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    # We can't use pkgs.bazel here, as it is somewhat outdated. It features
    # version 0.10.1, while rules_haskell (for example) requires bazel >= 0.14.
    bazel = pkgs.callPackage ./overrides/bazel {
      inherit (pkgs.darwin) cctools;
      inherit (pkgs.darwin.apple_sdk.frameworks) CoreFoundation CoreServices Foundation;
      buildJdk = pkgs.jdk8;
      buildJdkName = "jdk8";
      runJdk = pkgs.jdk8;
      stdenv = if pkgs.stdenv.cc.isClang then pkgs.llvmPackages_6.stdenv else pkgs.stdenv;
    };
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

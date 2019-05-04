# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs/nixos-19.03;

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
      # Create a C binary
      # Required by Bazel.
      # Added to nixpkgs in 88fe22d0d7d6626b7735a4a4e606215b951ad267
      writeCBin = name: code:
      pkgs.runCommandCC name
      {
        inherit name code;
        executable = true;
        passAsFile = ["code"];
          # Pointless to do this on a remote machine.
          preferLocalBuild = true;
          allowSubstitutes = false;
        }
        ''
          n=$out/bin/$name
          mkdir -p "$(dirname "$n")"
          mv "$codePath" code.c
          $CC -x c code.c -o "$n"
        '';
      };
      ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
        installPhase = ''
          mkdir -p $out
          PREFIX=$out make install
          wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql}/bin:$out/bin
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

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
      patches = [
        # Fix glibc version conflict.
        ./grpc-Fix-gettid-naming-conflict.patch
        ./grpc-Rename-gettid-functions.patch
      ];
    });
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_9_6}/bin:$out/bin
      '';
    });
    bazel = pkgs.bazel.overrideAttrs(oldAttrs: {
      patches = oldAttrs.patches ++ [
        # Note (MK)
        # This patch enables caching of tests marked as `exclusive`. It got apparently
        # rolled back because it caused problems internally at Google but it’s unclear
        # what is actually failing and it seems to work fine for us.
        # See https://github.com/bazelbuild/bazel/pull/8983/files#diff-107db037d4a55f2421fed9ed5c6cc31b
        # for the only change that actually affects the code in this patch. The rest is tests
        # and/or documentation.
        (pkgs.fetchurl {
          url = "https://patch-diff.githubusercontent.com/raw/bazelbuild/bazel/pull/8983.patch";
          sha256 = "1j25bycn9q7536ab3ln6yi6zpzv2b25fwdyxbgnalkpl2dz9idb7";
        })
      ];
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

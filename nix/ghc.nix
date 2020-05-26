# This file defines the GHC along with any overrides that is used for the
# nix builds and stack builds.

{ system ? builtins.currentSystem
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:

let
  hsLib = pkgs.haskell.lib;

  hsOverrides = with pkgs.haskell.lib; with pkgs.lib; (self: super:
    rec {
      # Override the default mkDerivation.
      # NOTE: Changing this in any way will cause a full rebuild of every package!
      mkDerivation = args: super.mkDerivation (args // {
          enableLibraryProfiling = args.enableLibraryProfiling or true;
          doCheck = args.doCheck or false;
      });
      withPackages = packages: super.callPackage ./with-packages-wrapper.nix {
          inherit (self) ghc llvmPackages;
          inherit packages;
      };
      ghcWithPackages = selectFrom: withPackages (selectFrom self);
  });

  ghc = pkgs.callPackage ./overrides/ghc-8.6.5.nix rec {
    bootPkgs = pkgs.haskell.packages.ghc865Binary;
    inherit (pkgs.python3Packages) sphinx;
    inherit (pkgs) buildLlvmPackages;
    enableIntegerSimple = true;
    enableShared = false;
    enableRelocatedStaticLibs = true;
    libffi = null;
  };

  packages = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules" {
    haskellLib = pkgs.haskell.lib;
    inherit ghc;
    buildHaskellPackages = packages;
    compilerConfig = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules/configuration-ghc-8.6.x.nix" { haskellLib = pkgs.haskell.lib; };
    overrides = hsOverrides;
  };

in packages

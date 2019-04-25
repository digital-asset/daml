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

      # We need newer versions that build with GHC 8.6
      language-c = super.callPackage ./overrides/language-c-0.8.2.nix {};
      c2hs = super.callPackage ./overrides/c2hs-0.28.6.nix {};
  });

  ghc863Binary = pkgs.callPackage ./overrides/ghc-8.6.3-binary.nix {
   gcc-clang-wrapper = "${toString pkgs.path}/pkgs/development/compilers/ghc/gcc-clang-wrapper.sh";
  };
  ghc863BinaryPackages = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules" {
    haskellLib = pkgs.haskell.lib;
    buildHaskellPackages = ghc863BinaryPackages;
    ghc = ghc863Binary;
    compilerConfig = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules/configuration-ghc-8.6.x.nix" { haskellLib = pkgs.haskell.lib; };
    packageSetConfig = self: super: {
      mkDerivation = drv: super.mkDerivation (drv // {
        doCheck = false;
        doHaddock = false;
        enableExecutableProfiling = false;
        enableLibraryProfiling = false;
        enableSharedExecutables = false;
        enableSharedLibraries = false;
      });
    };
  };

  ghc = pkgs.callPackage ./overrides/ghc-8.6.5.nix rec {
    bootPkgs = ghc863BinaryPackages;
    inherit (pkgs.python3Packages) sphinx;
    inherit (pkgs) buildLlvmPackages;
    enableIntegerSimple = true;
    enableRelocatedStaticLibs = true;
  };

  packages = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules" {
    haskellLib = pkgs.haskell.lib;
    inherit ghc;
    buildHaskellPackages = packages;
    compilerConfig = pkgs.callPackage "${toString pkgs.path}/pkgs/development/haskell-modules/configuration-ghc-8.6.x.nix" { haskellLib = pkgs.haskell.lib; };
    overrides = hsOverrides;
  };

in packages

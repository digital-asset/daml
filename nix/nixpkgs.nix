# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? import ./system.nix
, ...
}:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    nodejs = pkgs.nodejs-16_x;
    nodejs14 = pkgs.nodejs-14_x;
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_11}/bin:$out/bin
      '';
    });
    scala_2_13 = pkgs.scala_2_13.overrideAttrs (oldAttrs: rec {
      version = "2.13.10";
      name = "scala-2.13.10";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "sha256-sBRhWZzQeGBCxktTN5D0XlG6u5HFLcRl2EaDjpcBnMQ=";
      };
    });

    bazel_4 = pkgs.bazel_4.overrideAttrs(oldAttrs: {
      patches = oldAttrs.patches ++ [
        # Bazel is too aggressive in treating arguments starting with @ as
        # response files. This fix has been upstreamed and is available from Bazel 5.1.0
        # https://github.com/bazelbuild/bazel/commit/48b60d22bca0158d194b78481ff86b0ac251243f
        ./bazel-cc-wrapper-response-file.patch
        # This has been upstreamed but it's only available from Bazel 7.0.0-pre.20230215.2
        # (maybe we can get it backported?)
        # https://github.com/bazelbuild/bazel/commit/6115d94cd05864fe5c6e5f774e9482b3b4976976
        ./bazel-retry-cache.patch
      ];
    });
    haskell = pkgs.haskell // {
      compiler = pkgs.haskell.compiler // {
        ghc902 =
          if system == "aarch64-darwin" then
            pkgs.haskell.compiler.ghc902.override(oldAttrs: {
              buildTargetLlvmPackages = pkgs.llvmPackages_12;
              llvmPackages = pkgs.llvmPackages_12;
            })
          else
            pkgs.haskell.compiler.ghc902;
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

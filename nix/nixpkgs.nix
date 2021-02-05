# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem, ... }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    # nixpkgs ships with a very old version of openjdk on darwin
    # because newer versions cause issues with jvmci. We don’t care
    # about that so we upgrade it to the latest.
    jdk8 =
      if pkgs.stdenv.isDarwin then
        pkgs.jdk8.overrideAttrs(oldAttrs: {
          name = "zulu1.8.0_282-8.52.0.23";
          src = pkgs.fetchurl {
            url = "https://cdn.azul.com/zulu/bin/zulu8.52.0.23-ca-jdk8.0.282-macosx_x64.zip";
            sha256 = "04azr412azqx3cyj9fda0r025hbzypwbnpb44gi15s683ns63wd2";
            curlOpts = "-H Referer:https://www.azul.com/downloads/zulu/zulu-linux/";
          };
        })
      else pkgs.jdk8;
    nodejs = pkgs.nodejs-12_x;
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
          sha256 = "1qdyqymsylinkdwqhbxbm8bvbyznrdn74n744pi5xhdwb6lw1r8a";
        })
      ];
    });
    scala_2_12 = pkgs.scala_2_12.overrideAttrs (oldAttrs: rec {
      version = "2.12.12";
      name = "scala-2.12.12";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "3520cd1f3c9efff62baee75f32e52d1e5dc120be2ccf340649e470e48f527e2b";
      };
    });
   haskell = pkgs.haskell // {
     packages = pkgs.haskell.packages // {
       integer-simple = pkgs.haskell.packages.integer-simple // {
        ghc8103 = pkgs.haskell.packages.ghc8103.override {
          ghc = pkgs.haskell.compiler.ghc8103.overrideAttrs (old: {
            postInstall = ''
    # Install the bash completion file.
    install -D -m 444 utils/completion/ghc.bash $out/share/bash-completion/completions/ghc

    # Patch scripts to include "readelf" and "cat" in $PATH.
    for i in "$out/bin/"*; do
      test ! -h $i || continue
      egrep --quiet '^#!' <(head -n 1 $i) || continue
      sed -i -e '2i export PATH="$PATH:${pkgs.lib.makeBinPath ([ pkgs.targetPackages.stdenv.cc.bintools pkgs.coreutils ] ++ pkgs.stdenv.lib.optional pkgs.targetPlatform.isDarwin pkgs.darwin.cctools) }"' $i
    done
  '';
          });
        };
       };
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

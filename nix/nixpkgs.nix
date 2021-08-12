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
    scala_2_12 = pkgs.scala_2_12.overrideAttrs (oldAttrs: rec {
      version = "2.12.14";
      name = "scala-2.12.14";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "09h4g3lrj45j3nc0sb3nk7jb1xzsjz2lf50akji170186903wzpx";
      };
    });
    scala_2_13 = pkgs.scala_2_13.overrideAttrs (oldAttrs: rec {
      version = "2.13.5";
      name = "scala-2.13.5";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "1ah5rw6xqksiayi5i95r3pcff961q71ilishzn2kmg673z0j2b7d";
      };
    });
   haskell = pkgs.haskell // {
     packages = pkgs.haskell.packages // {
       integer-simple = pkgs.haskell.packages.integer-simple // {
        ghc8104 = pkgs.haskell.packages.integer-simple.ghc8104.override {
          ghc = pkgs.haskell.compiler.integer-simple.ghc8104.overrideAttrs (old: {
            # We need to include darwin.cctools in PATH to make sure GHC finds
            # otool.
            postInstall = ''
    # Install the bash completion file.
    install -D -m 444 utils/completion/ghc.bash $out/share/bash-completion/completions/ghc

    # Patch scripts to include "readelf" and "cat" in $PATH.
    for i in "$out/bin/"*; do
      test ! -h $i || continue
      egrep --quiet '^#!' <(head -n 1 $i) || continue
      sed -i -e '2i export PATH="$PATH:${pkgs.lib.makeBinPath ([ pkgs.targetPackages.stdenv.cc.bintools pkgs.coreutils ] ++ pkgs.lib.optional pkgs.targetPlatform.isDarwin pkgs.darwin.cctools) }"' $i
    done
  '';
          });
        };
       };
     };
    };

    bazel_4 = pkgs.bazel_4.overrideAttrs(oldAttrs: {
      patches = oldAttrs.patches ++ [
        # This should be upstreamed. Bazel is too aggressive
        # in treating arguments starting with @ as response files.
        ./bazel-cc-wrapper-response-file.patch
        # This should be upstreamed once we tested it a bit
        # on our own setup.
        ./bazel-retry-cache.patch
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

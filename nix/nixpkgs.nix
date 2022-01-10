# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem, ... }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    nodejs = pkgs.nodejs-12_x;
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_10}/bin:$out/bin
      '';
    });
    scala_2_12 = pkgs.scala_2_12.overrideAttrs (oldAttrs: rec {
      version = "2.12.15";
      name = "scala-2.12.15";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "17945e3ca9478d06a8436056aac6b9afdf60deafdc3e382c6c08b603921b7ab6";
      };
    });
    scala_2_13 = pkgs.scala_2_13.overrideAttrs (oldAttrs: rec {
      version = "2.13.6";
      name = "scala-2.13.6";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "0hzd6pljc8z5fwins5a05rwpx2w7wmlb6gb8973c676i7i895ps9";
      };
    });
   haskell = pkgs.haskell // {
     packages = pkgs.haskell.packages // {
       integer-simple = pkgs.haskell.packages.integer-simple // {
        ghc8107 = pkgs.haskell.packages.integer-simple.ghc8107.override {
          ghc = pkgs.haskell.compiler.integer-simple.ghc8107.overrideAttrs (old: {
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
        # This fixes an issue where protoc segfaults on MacOS Monterey
        (pkgs.fetchpatch {
          url = "https://github.com/bazelbuild/bazel/commit/ae0a6c98d4f94abedbedb2d51c27de5febd7df67.patch";
          sha256 = "sha256-YcdxqjTMGI86k1wgFqxJqghv0kknAjlFQFpt4VccCTE=";
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

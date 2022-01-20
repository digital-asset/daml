# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? builtins.currentSystem, ... }:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  # package overrides
  overrides = _: pkgs: rec {
    nodejs = pkgs.nodejs-14_x;
    ephemeralpg = pkgs.ephemeralpg.overrideAttrs(oldAttrs: {
      installPhase = ''
        mkdir -p $out
        PREFIX=$out make install
        wrapProgram $out/bin/pg_tmp --prefix PATH : ${pkgs.postgresql_10}/bin:$out/bin
      '';
    });
    scala_2_13 = pkgs.scala_2_13.overrideAttrs (oldAttrs: rec {
      version = "2.13.8";
      name = "scala-2.13.8";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "0hzd6pljc8z5fwins5a05rwpx2w7wmlb6gb8973c676i7i895ps9";
      };
    });

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

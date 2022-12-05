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
      version = "2.13.8";
      name = "scala-2.13.8";
      src = pkgs.fetchurl {
        url = "https://www.scala-lang.org/files/archive/${name}.tgz";
        sha256 = "1kql2gh9s6xy0r4zalk7f8qx0l35n0d7m0ww1sgq6lf6d621vcrc";
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

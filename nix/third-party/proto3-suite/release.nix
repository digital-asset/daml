# To develop iteratively within this repository, open a Nix shell via:
#
#     $ nix-shell -A proto3-suite.env release.nix
#
# ... and then use `cabal` to build and test:
#
#     [nix-shell]$ cabal configure --enable-tests
#     [nix-shell]$ cabal build
#     [nix-shell]$ cabal test

let
  nixpkgs = import ./nixpkgs/17_09.nix;

  config = {
    allowUnfree = true;

    packageOverrides = pkgs: {
      haskellPackages = pkgs.haskellPackages.override {
        overrides = self: super: rec {

          # The test suite for proto3-suite requires:
          #
          #   - a GHC with `proto3-suite` installed, since our code generation
          #     tests compile and run generated code; since this custom GHC is
          #     also used inside the nix-shell environment for iterative
          #     development, we ensure that it is available on $PATH and that
          #     all test suite deps are available to it
          #
          #   - a Python interpreter with a protobuf package installed, which we
          #     use as a reference implementation; we also put expose this on
          #     the `nix-shell` $PATH
          #
          # Finally, we make `cabal` available in the `nix-shell`, intentionally
          # occluding any globally-installed versions of the tool.

          proto3-suite =
            pkgs.haskell.lib.overrideCabal
              (self.callPackage ./default.nix { })
              (drv:
                 let
                   python = pkgs.python.withPackages (pkgs: [ pkgs.protobuf3_0 ]);
                 in
                   {
                     shellHook = (drv.shellHook or "") +
                       (let
                          ghc = self.ghcWithPackages (pkgs:
                            drv.testHaskellDepends ++ [ pkgs.proto3-suite-boot ]
                          );
                        in ''
                          export PATH=${self.cabal-install}/bin:${ghc}/bin:${python}/bin''${PATH:+:}$PATH
                        '');

                     testHaskellDepends = drv.testHaskellDepends ++ [
                       pkgs.ghc
                       proto3-suite-boot
                       python
                     ];
                   }
              );

          # A proto3-suite sans tests, for bootstrapping
          proto3-suite-boot =
            pkgs.haskell.lib.overrideCabal
              (self.callPackage ./default.nix { })
              (_: {
                 configureFlags = [ "--disable-optimization" ];
                 doCheck        = false;
                 doHaddock      = false;
               });

          proto3-wire =
            self.callPackage ./nix/proto3-wire.nix { };

          swagger2 =
            pkgs.haskell.lib.dontCheck
              (pkgs.haskell.lib.dontHaddock
                (self.callPackage ./nix/swagger2.nix { }));
        };
      };
    };
  };
in

let
   linuxPkgs = import nixpkgs { inherit config; system = "x86_64-linux" ; };
  darwinPkgs = import nixpkgs { inherit config; system = "x86_64-darwin"; };
        pkgs = import nixpkgs { inherit config; };
in
  { proto3-suite-linux    =     linuxPkgs.haskellPackages.proto3-suite;
    proto3-suite-darwin   =    darwinPkgs.haskellPackages.proto3-suite;
    proto3-suite          =          pkgs.haskellPackages.proto3-suite;
    proto3-suite-no-tests = pkgs.haskellPackages.proto3-suite-no-tests;
  }

# If you would like to test and build changes quickly using `cabal`, run:
#
#     $ # Consider adding the following command to your `~/.profile`
#     $ NIX_PATH="${NIX_PATH}:ssh-config-file=${HOME}/.ssh/config:ssh-auth-sock=${SSH_AUTH_SOCK}"
#     $ nix-shell -A grpc-haskell.env release.nix
#     [nix-shell]$ cabal configure --enable-tests && cabal build && cabal test
#
# This will open up a Nix shell where all of your Haskell tools will work like
# normal, except that all dependencies (including C libraries) are managed by
# Nix.  The only thing that won't work is running tests inside this shell
# (although you can still build them).  Fixing the test suite requires
# extensive patching of the test scripts (see `postPatch` below)
#
# Note that this will compile the library once without tests using Nix.  This
# is due to the fact that `grpc-haskell`'s test suite cannot test code
# generation without the library being built at least once.
#
# If you want to build and test this repository using `nix`, you can run the
# following command:
#
#     $ nix-build -A grpc-haskell release.nix
#
# ... but this is not recommended for normal development because this will
# rebuild the repository from scratch every time, which is extremely slow.  Only
# do this if you want to exactly reproduce our continuous integration build.
#
# If you update the `grpc-haskell.cabal` file (such as changing dependencies or
# adding new library/executable/test/benchmark sections), then update the
# `default.nix` expression by running:
#
#     $ cabal2nix . > default.nix
#
# By default, Nix will pick a version for each one of your Haskell dependencies.
# If you would like to select a different version then, run:
#
#     $ cabal2nix cabal://${package-name}-${version} > nix/${package-name}.nix
#
# ... and then add this line below in the Haskell package overrides section:
#
#     ${package-name} =
#       haskellPackagesNew.callPackage ./nix/${package-name}.nix { };
#
# ... replacing `${package-name}` with the name of the package that you would
# like to upgrade and `${version}` with the version you want to upgrade to.
#
# You can also add private Git dependencies in the same way, except supplying
# the `git` URL to clone:
#
#     $ cabal2nix <your private git url>/${package-name}.git > ./nix/${package-name}.nix
#
# ...but also be sure to supply `fetchgit = pkgs.fetchgitPrivate` in the
# `haskellPackagesNew.callPackage` invocation for your private package.
#
# Note that `cabal2nix` also takes an optional `--revision` flag if you want to
# pick a revision other than the latest to depend on.
#
# Finally, if you want to test a local source checkout of a dependency, then
# run:
#
#     $ cabal2nix path/to/dependency/repo > nix/${package-name}.nix
let
  nixpkgs = import ./nixpkgs.nix;
  config = {
    packageOverrides = pkgs: rec {
      protobuf3_2NoCheck =
        pkgs.stdenv.lib.overrideDerivation
          pkgs.pythonPackages.protobuf3_2
          (oldAttrs : {doCheck = false; doInstallCheck = false;});

      cython = pkgs.pythonPackages.buildPythonPackage rec {
        name = "Cython-${version}";
        version = "0.24.1";

        src = pkgs.fetchurl {
          url = "mirror://pypi/C/Cython/${name}.tar.gz";
          sha256 = "84808fda00508757928e1feadcf41c9f78e9a9b7167b6649ab0933b76f75e7b9";
        };

        # This workaround was taken from https://github.com/NixOS/nixpkgs/issues/18729
        # This was fixed in `nixpkgs-unstable` so we can get rid of this workaround
        # when that fix is stabilized
        NIX_CFLAGS_COMPILE =
          pkgs.stdenv.lib.optionalString (pkgs.stdenv.cc.isClang or false)
            "-I${pkgs.libcxx}/include/c++/v1";

        buildInputs =
              pkgs.stdenv.lib.optional (pkgs.stdenv.cc.isClang or false) pkgs.libcxx
          ++  [ pkgs.pkgconfig pkgs.gdb ];

        doCheck = false;

        doHaddock = false;

        doHoogle = false;

        meta = {
          description = "An optimising static compiler for both the Python programming language and the extended Cython programming language";
          platforms = pkgs.stdenv.lib.platforms.all;
          homepage = http://cython.org;
          license = pkgs.stdenv.lib.licenses.asl20;
          maintainers = with pkgs.stdenv.lib.maintainers; [ fridh ];
        };
      };

      grpc = pkgs.callPackage ./nix/grpc.nix { };

      grpcio = pkgs.pythonPackages.buildPythonPackage rec {
        name = "grpc-${version}";

        version = "1.0";

        src = pkgs.fetchgit {
          url    = "https://github.com/grpc/grpc.git";
          rev    = "e2cfe9df79c4eda4e376222df064c4c65e616352";
          sha256 = "19ldbjlnbc287hkaylsigm8w9fai2bjdbfxk6315kl75cq54iprr";
        };

        preConfigure = ''
          export GRPC_PYTHON_BUILD_WITH_CYTHON=1
        '';

        # This workaround was taken from https://github.com/NixOS/nixpkgs/issues/18729
        # This was fixed in `nixpkgs-unstable` so we can get rid of this workaround
        # when that fix is stabilized
        NIX_CFLAGS_COMPILE =
          pkgs.stdenv.lib.optionalString (pkgs.stdenv.cc.isClang or false)
            "-I${pkgs.libcxx}/include/c++/v1";

        buildInputs =
          pkgs.stdenv.lib.optional (pkgs.stdenv.cc.isClang or false) pkgs.libcxx;

        propagatedBuildInputs = [
          cython
          pkgs.pythonPackages.futures
          protobuf3_2NoCheck
          pkgs.pythonPackages.enum34
        ];
      };

      grpcio-tools = pkgs.pythonPackages.buildPythonPackage rec {
        name = "grpc-${version}";

        version = "1.0";

        src = pkgs.fetchgit {
          url    = "https://github.com/grpc/grpc.git";
          rev    = "e2cfe9df79c4eda4e376222df064c4c65e616352";
          sha256 = "19ldbjlnbc287hkaylsigm8w9fai2bjdbfxk6315kl75cq54iprr";
        };

        preConfigure = ''
          export GRPC_PYTHON_BUILD_WITH_CYTHON=1
          cd tools/distrib/python/grpcio_tools
          python ../make_grpcio_tools.py
        '';

        # This workaround was taken from https://github.com/NixOS/nixpkgs/issues/18729
        # This was fixed in `nixpkgs-unstable` so we can get rid of this workaround
        # when that fix is stabilized
        NIX_CFLAGS_COMPILE =
          pkgs.stdenv.lib.optionalString (pkgs.stdenv.cc.isClang or false)
            "-I${pkgs.libcxx}/include/c++/v1";

        buildInputs =
          pkgs.stdenv.lib.optional (pkgs.stdenv.cc.isClang or false) pkgs.libcxx;

        propagatedBuildInputs = [
          cython
          pkgs.pythonPackages.futures
          protobuf3_2NoCheck
          pkgs.pythonPackages.enum34
          grpcio
        ];
      };

      usesGRPC = haskellPackage:
        pkgs.haskell.lib.overrideCabal haskellPackage (oldAttributes: {
            preBuild = (oldAttributes.preBuild or "") +
              pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                export DYLD_LIBRARY_PATH=${grpc}/lib''${DYLD_LIBRARY_PATH:+:}$DYLD_LIBRARY_PATH
              '';

            shellHook = (oldAttributes.shellHook or "") +
              pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                export DYLD_LIBRARY_PATH=${grpc}/lib''${DYLD_LIBRARY_PATH:+:}$DYLD_LIBRARY_PATH
              '';
          }
        );

      haskellPackages = pkgs.haskellPackages.override {
        overrides = haskellPackagesNew: haskellPackagesOld: rec {
          aeson =
            pkgs.haskell.lib.dontCheck
              (haskellPackagesNew.callPackage ./nix/aeson.nix {});

          cabal-doctest =
            haskellPackagesNew.callPackage ./nix/cabal-doctest.nix { };

          insert-ordered-containers =
            haskellPackagesNew.callPackage ./nix/insert-ordered-containers.nix { };

          optparse-applicative =
            haskellPackagesNew.callPackage ./nix/optparse-applicative.nix { };

          optparse-generic =
            haskellPackagesNew.callPackage ./nix/optparse-generic.nix { };

          proto3-wire =
            haskellPackagesNew.callPackage ./nix/proto3-wire.nix { };

          proto3-suite =
            pkgs.haskell.lib.dontCheck
              (haskellPackagesNew.callPackage ./nix/proto3-suite.nix {});

          grpc-haskell-core =
            usesGRPC (haskellPackagesNew.callPackage ./core { });

          grpc-haskell-no-tests =
            usesGRPC
              (pkgs.haskell.lib.dontCheck
                (haskellPackagesNew.callPackage ./default.nix { })
              );

          grpc-haskell =
            usesGRPC
              (pkgs.haskell.lib.overrideCabal
                (haskellPackagesNew.callPackage ./default.nix { })
                (oldDerivation:
                  let
                    ghc =
                      haskellPackagesNew.ghcWithPackages (pkgs: [
                        pkgs.grpc-haskell-no-tests
                        # Include some additional packages in this custom ghc for
                        # running tests in the nix-shell environment.
                        pkgs.tasty-quickcheck
                        pkgs.turtle
                      ]);

                    python = pkgs.python.withPackages (pkgs: [
                      # pkgs.protobuf3_0
                      grpcio-tools
                    ]);

                  in rec {
                    buildDepends = [
                      pkgs.makeWrapper
                      # Give our nix-shell its own cabal so we don't pick up one
                      # from the user's environment by accident.
                      haskellPackagesNew.cabal-install
                    ];

                    patches = [ tests/tests.patch ];

                    postPatch = ''
                      patchShebangs tests
                      substituteInPlace tests/simple-client.sh \
                        --replace @makeWrapper@ ${pkgs.makeWrapper} \
                        --replace @grpc@ ${grpc}
                      substituteInPlace tests/simple-server.sh \
                        --replace @makeWrapper@ ${pkgs.makeWrapper} \
                        --replace @grpc@ ${grpc}
                      wrapProgram tests/protoc.sh \
                        --prefix PATH : ${python}/bin
                      wrapProgram tests/test-client.sh \
                        --prefix PATH : ${python}/bin
                      wrapProgram tests/test-server.sh \
                        --prefix PATH : ${python}/bin
                      wrapProgram tests/simple-client.sh \
                        --prefix PATH : ${ghc}/bin
                      wrapProgram tests/simple-server.sh \
                        --prefix PATH : ${ghc}/bin
                    '';

                    shellHook = (oldDerivation.shellHook or "") + ''
                      # This lets us use our custom ghc and python environments in the shell.
                      export PATH=${ghc}/bin:${python}/bin''${PATH:+:}$PATH
                    '';
                  })
              );

          swagger2 =
            pkgs.haskell.lib.dontCheck (pkgs.haskell.lib.dontHaddock (haskellPackagesNew.callPackage ./nix/swagger2.nix { }));

          turtle =
            haskellPackagesNew.callPackage ./nix/turtle.nix { };

        };
      };
    };

    allowUnfree = true;
  };

in

let
   linuxPkgs = import nixpkgs { inherit config; system = "x86_64-linux" ; };
  darwinPkgs = import nixpkgs { inherit config; system = "x86_64-darwin"; };
        pkgs = import nixpkgs { inherit config; };

in
  { grpc-haskell-linux    =  linuxPkgs.haskellPackages.grpc-haskell;
    grpc-haskell-darwin   = darwinPkgs.haskellPackages.grpc-haskell;
    grpc-haskell          =       pkgs.haskellPackages.grpc-haskell;
    grpc-haskell-no-tests = pkgs.haskellPackages.grpc-haskell-no-tests;
    grpc-linux            =  linuxPkgs.grpc;
    grpc-darwin           = darwinPkgs.grpc;
    grpc                  =       pkgs.grpc;
  }

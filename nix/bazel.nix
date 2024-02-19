# Bazel MUST only use this file to source dependencies
#
# This allows CI to pre-build and cache the build outputs
{ system ? import ./system.nix
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:
let shared = rec {
  inherit (pkgs)
    buf
    coreutils
    curl
    diffutils
    docker
    gawk
    gnupatch
    gnutar
    grpcurl
    gzip
    imagemagick
    jdk17
    jdk8
    jekyll
    jq
    netcat-gnu
    openssl
    patchelf
    protobuf3_8
    python3
    toxiproxy
    zip
  ;

    # ghc-lib needs GHC 8.6.5, but the first version to support Darwin arm64 /
    # Apple M1 is GHC 8.10.7. Use GHC 8.6.5 on aarch64_darwin via Rosetta to build
    # the ghc-lib cabal sdist.
    ghc_lib_pkgs = if system == "aarch64-darwin" then import ./nixpkgs.nix { system = "x86_64-darwin"; } else pkgs;

    ghc_lib_deps = with ghc_lib_pkgs; buildEnv {
      name = "ghc-lib-deps";
      paths = [
        autoconf
        automake
        cabal-install
        diffutils
        git
        gnumake
        ncurses
        perl
        haskell.compiler.ghc865Binary
        stdenv.cc  # ghc-lib needs `gcc` or `clang`, but Bazel provides `cc`.
        xz
      ] ++ (
        if stdenv.isDarwin
        then [stdenv.cc.bintools.bintools xcodebuild]
        else []
      );
      ignoreCollisions = stdenv.isDarwin;
    };

    postgresql_12 = if pkgs.buildPlatform.libc == "glibc"
      then pkgs.runCommand "postgresql_12_wrapper" { buildInputs = [ pkgs.makeWrapper ]; } ''
      mkdir -p $out/bin
      for tool in ${pkgs.postgresql_12}/bin/*; do
        makeWrapper $tool $out/bin/$(basename $tool) --set LOCALE_ARCHIVE ${pkgs.glibcLocales}/lib/locale/locale-archive
      done
      ln -s ${pkgs.postgresql_12}/include $out/include
      ln -s ${pkgs.postgresql_12}/lib $out/lib
      ln -s ${pkgs.postgresql_12}/share $out/share
    '' else pkgs.postgresql_12;


    scala_2_13 = (pkgs.scala_2_13.override { }).overrideAttrs (attrs: {
      # Something appears to be broken in nixpkgs' fixpoint which results in the
      # test not having the version number we overwrite so it fails
      # with a mismatch between the version in nixpkgs and the one we
      # overwrite.
      installCheckPhase = "";
      nativeBuildInputs = attrs.nativeBuildInputs ++ [ pkgs.makeWrapper ];
      installPhase = attrs.installPhase + ''
        wrapProgram $out/bin/scala    --add-flags "-nobootcp"
        wrapProgram $out/bin/scalac   --add-flags "-nobootcp"
        wrapProgram $out/bin/scaladoc --add-flags "-nobootcp"
        wrapProgram $out/bin/scalap   --add-flags "-nobootcp"
      '';
    });

  # We need to have a file in GOPATH that we can use as
  # root_file in go_wrap_sdk.
  go = pkgs.buildEnv {
    name = "bazel-go-toolchain";
    paths = [
      pkgs.go
    ];
    postBuild = ''
      touch $out/ROOT
      ln -s $out/share/go/{api,doc,lib,misc,pkg,src} $out/
    '';
  };

  ghcPkgs = pkgs.haskell.packages.native-bignum.ghc902;

  ghc =
    if system == "aarch64-darwin" then
      pkgs.runCommand "ghc-aarch64-symlinks" { buildInputs = [ pkgs.makeWrapper ]; } ''
        mkdir -p $out/bin
        for tool in \
          ghc-9.0.2 \
          ghc-pkg \
          ghc-pkg-9.0.2 \
          ghci \
          ghci-9.0.2 \
          haddock \
          hp2ps \
          hpc \
          runghc-9.0.2 \
          runhaskell
        do
            ln -s ${ghcPkgs.ghc}/bin/$tool $out/bin/$tool
        done;
        mkdir -p $out/lib
        ln -s ${ghcPkgs.ghc}/lib/ghc-9.0.2 $out/lib/ghc-9.0.2
        makeWrapper ${ghcPkgs.ghc}/bin/ghc $out/bin/ghc \
          --set CODESIGN_ALLOCATE ${pkgs.darwin.cctools}/bin/codesign_allocate \
          --prefix PATH : ${pkgs.llvmPackages_12.clang}/bin:${pkgs.llvmPackages_12.llvm}/bin
        makeWrapper ${ghcPkgs.ghc}/bin/runghc $out/bin/runghc \
          --set CODESIGN_ALLOCATE ${pkgs.darwin.cctools}/bin/codesign_allocate \
          --prefix PATH : ${pkgs.llvmPackages_12.clang}/bin:${pkgs.llvmPackages_12.llvm}/bin
        makeWrapper ${ghcPkgs.ghc}/bin/hsc2hs $out/bin/hsc2hs \
          --set CODESIGN_ALLOCATE ${pkgs.darwin.cctools}/bin/codesign_allocate \
          --prefix PATH : ${pkgs.llvmPackages_12.clang}/bin:${pkgs.llvmPackages_12.llvm}/bin
      ''
    else
      ghcPkgs.ghc;


  # Deliberately not taken from ghcPkgs. This is a fully
  # static executable so it doesn’t pull in another GHC
  # and upstream nixpkgs does not cache packages for
  # integer-simple.
  hlint = pkgs.hlint;

  # Java 8 development
  mvn = pkgs.writeScriptBin "mvn" ''
    exec ${pkgs.maven}/bin/mvn ''${MVN_SETTINGS:+-s "$MVN_SETTINGS"} "$@"
  '';

  # rules_nodejs expects nodejs in a subdirectory of a repository rule.
  # We use a linkFarm to fulfill this requirement.
  nodejs = pkgs.nodejs-16_x;
  nodejsNested = pkgs.linkFarm "nodejs" [ { name = "node_nix"; path = nodejs; }];
  nodejs14Nested = pkgs.linkFarm "nodejs" [ { name = "node_nix"; path = pkgs.nodejs-14_x; }];

  sass = pkgs.sass;

  sphinx-copybutton = pkgs.python3Packages.buildPythonPackage rec {
      pname = "sphinx-copybutton";
      version = "0.2.12";

      src = pkgs.python3Packages.fetchPypi {
        inherit pname version;
        sha256 = "0p1yls8pplfg59wzmb96m3pjcyr3202an1rcr5wn2jwqhqvqi4ll";
      };
      doCheck = false;
      buildInputs = [pkgs.python3Packages.sphinx];
  } ;

  sphinx-exts = pkgs.python3Packages.sphinx.overridePythonAttrs (attrs: rec {
    propagatedBuildInputs = attrs.propagatedBuildInputs ++ [sphinx-copybutton];
  });

  script = pkgs.unixtools.script;
  sysctl = pkgs.unixtools.sysctl;

  # Custom combination of latex packages for our latex needs
  texlive = pkgs.texlive.combine {
    inherit (pkgs.texlive)
      bera
      capt-of
      collection-fontsrecommended
      collection-luatex
      datetime
      enumitem
      environ
      epigraph
      eqparbox
      eulervm
      fancyhdr
      fmtcount
      fncychap
      footmisc
      footnotebackref
      framed
      latexmk
      lipsum
      mathpartir
      mathpazo
      mnsymbol
      multirow
      needspace
      palatino
      scheme-small
      tabulary
      threeparttable
      tikzsymbols
      titlesec
      tocbibind
      todonotes
      transparent
      trimspaces
      varwidth
      wrapfig
      xargs
    ;
  };

  bazel-cc-toolchain = pkgs.callPackage ./tools/bazel-cc-toolchain { sigtool = pkgs.darwin.sigtool; };
};
in shared // (if pkgs.stdenv.isLinux then {
  inherit (pkgs)
    glibcLocales
    ;
  } else {})

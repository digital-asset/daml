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
    docker
    gawk
    gnutar
    grpcurl
    gzip
    imagemagick
    jdk8
    jdk11
    jekyll
    jq
    netcat-gnu
    nodejs
    openssl
    gnupatch
    patchelf
    protobuf3_8
    python3
    toxiproxy
    zip
  ;

    postgresql_10 = if pkgs.buildPlatform.libc == "glibc"
      then pkgs.runCommand "postgresql_10_wrapper" { buildInputs = [ pkgs.makeWrapper ]; } ''
      mkdir -p $out/bin
      for tool in ${pkgs.postgresql_10}/bin/*; do
        makeWrapper $tool $out/bin/$(basename $tool) --set LOCALE_ARCHIVE ${pkgs.glibcLocales}/lib/locale/locale-archive
      done
      ln -s ${pkgs.postgresql_10}/include $out/include
      ln -s ${pkgs.postgresql_10}/lib $out/lib
      ln -s ${pkgs.postgresql_10}/share $out/share
    '' else pkgs.postgresql_10;


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

  ghc = ghcPkgs.ghc;
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
  nodejsNested = pkgs.linkFarm "nodejs" [ { name = "node_nix"; path = pkgs.nodejs; }];

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

  bazel-cc-toolchain = pkgs.callPackage ./tools/bazel-cc-toolchain {};
};
in shared // (if pkgs.stdenv.isLinux then {
  inherit (pkgs)
    glibcLocales
    ;
  } else {})

# Bazel MUST only use this file to source dependencies
#
# This allows CI to pre-build and cache the build outputs
{ system ? builtins.currentSystem
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:
let shared = rec {
  inherit (pkgs)
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

    postgresql_9_6 = if pkgs.buildPlatform.libc == "glibc"
      then pkgs.runCommand "postgresql_9_6_wrapper" { buildInputs = [ pkgs.makeWrapper ]; } ''
      mkdir -p $out/bin
      for tool in ${pkgs.postgresql_9_6}/bin/*; do
        makeWrapper $tool $out/bin/$(basename $tool) --set LOCALE_ARCHIVE ${pkgs.glibcLocales}/lib/locale/locale-archive
      done
      ln -s ${pkgs.postgresql_9_6}/include $out/include
      ln -s ${pkgs.postgresql_9_6}/lib $out/lib
      ln -s ${pkgs.postgresql_9_6}/share $out/share
    '' else pkgs.postgresql_9_6;


    scala_2_12 = (pkgs.scala_2_12.override { }).overrideAttrs (attrs: {
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
  go = pkgs.go.overrideAttrs (oldAttrs: {
    doCheck = false;
    postFixup = ''touch $out/share/go/ROOT'';
  });

  ghcPkgs = pkgs.haskell.packages.integer-simple.ghc8107;

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
      buildInputs = [sphinx183];
  } ;

  # sphinx 2.2.2 causes build failures of //docs:pdf-docs.
  # We override here rather than in nixpkgs.nix since GHC depends on sphinx
  # and we don’t want to rebuild that unnecessarily.
  sphinx183 = pkgs.python3Packages.sphinx.overridePythonAttrs (attrs: rec {
    version = "1.8.3";
    doCheck = false;
    src = pkgs.fetchFromGitHub {
      owner = "sphinx-doc";
      repo = "sphinx";
      rev = "v${version}";
      sha256 = "1hkqi5kzs85idv1w85qdl1bb2fwh7ccmgp6m860kzpkrl55149y8";
    };
  });

  sphinx183-exts = sphinx183.overridePythonAttrs (attrs: rec {
    propagatedBuildInputs = attrs.propagatedBuildInputs ++ [sphinx-copybutton];
  });

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

  z3 = pkgs.z3;

  bazel-cc-toolchain = pkgs.callPackage ./tools/bazel-cc-toolchain {};
};
in shared // (if pkgs.stdenv.isLinux then {
  inherit (pkgs)
    glibcLocales
    ;
  } else {})

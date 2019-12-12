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
    grpc
    grpcurl
    gzip
    hlint
    imagemagick
    jdk8
    jq
    netcat-gnu
    nodejs
    patchelf
    postgresql_9_6
    protobuf3_8
    python3
    zip
    ;

  scala = pkgs.scala_2_12;

  # We need to have a file in GOPATH that we can use as
  # root_file in go_wrap_sdk.
  go = pkgs.go.overrideAttrs (oldAttrs: {
    doCheck = false;
    postFixup = ''touch $out/share/go/ROOT'';
  });

  ghc = pkgs.haskell.packages.ghc865;

  # GHC configured for static linking only.
  ghcStatic = (import ./ghc.nix { inherit pkgs; }).ghc.override { enableShared = false; };


  # Java 8 development
  mvn = pkgs.writeScriptBin "mvn" ''
    exec ${pkgs.maven}/bin/mvn ''${MVN_SETTINGS:+-s "$MVN_SETTINGS"} "$@"
  '';

  sass = pkgs.sass;

  sphinx183 = pkgs.python3Packages.sphinx;

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
  ghcStaticDwarf = shared.ghcStatic.override { enableDwarf = true; };
  } else {})

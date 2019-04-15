# Bazel MUST only use this file to source dependencies
#
# This allows CI to pre-build and cache the build outputs
{ system ? builtins.currentSystem
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:
rec {
  inherit (pkgs)
    curl
    docker
    gawk
    gnutar
    go
    gzip
    hlint
    imagemagick
    jdk8
    jq
    libffi
    nodejs
    patchelf
    protobuf3_5
    zip
    ;

  # the GHC version we use plus custom overrides to sync with the
  # stackage version as specified in stack.yaml. Prefer to use this for
  # haskell binaries to keep the dev-env closure size as small
  # as possible.
  ghc = import ./ghc.nix { inherit pkgs; };

  # GHC with the "c2hs" package included.
  ghcWithC2hs = ghc.ghcWithPackages (p: [p.c2hs]);


  # Java 8 development
  mvn = pkgs.writeScriptBin "mvn" ''
    exec ${pkgs.maven}/bin/mvn ''${MVN_SETTINGS:+-s "$MVN_SETTINGS"} "$@"
  '';

  # The sass derivation in nixos-18.09 is broken, so we add our own
  # created with bundix.
  sass = pkgs.callPackage ./overrides/sass {};

  sphinx183 = import ./tools/sphinx183 {
    inherit pkgs;
    pythonPackages = pkgs.python36Packages;
  };

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
} // (if pkgs.stdenv.isLinux then {
  inherit (pkgs)
    glibcLocales
    ;
  } else {})

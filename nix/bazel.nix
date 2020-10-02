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
    imagemagick
    jdk8
    jekyll
    jq
    netcat-gnu
    nodejs
    openssl
    gnupatch
    patchelf
    postgresql_9_6
    protobuf3_8
    python3
    toxiproxy
    zip
    ;

  scala = pkgs.scala_2_12;

  # We need to have a file in GOPATH that we can use as
  # root_file in go_wrap_sdk.
  go = pkgs.go.overrideAttrs (oldAttrs: {
    doCheck = false;
    postFixup = ''touch $out/share/go/ROOT'';
  });

  # GHC configured for static linking only.
  ghcStaticPkgs = (import ./ghc.nix { inherit pkgs; }).override {
    overrides = self: super: {
      mkDerivation = args: super.mkDerivation (args // {
        enableLibraryProfiling = false;
        doHoogle = false;
        doHaddock = false;
        doCheck = false;
      });
      hlint = pkgs.haskell.lib.justStaticExecutables super.hlint;
    };
  };
  ghcStatic = ghcStaticPkgs.ghc;
  hlint = ghcStaticPkgs.hlint;


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
    src = attrs.src.override {
      inherit version;
      sha256 = "c4cb17ba44acffae3d3209646b6baec1e215cad3065e852c68cc569d4df1b9f8";
    };
  });

  sphinx183-exts = sphinx183.overridePythonAttrs (attrs: rec {
    propagatedBuildInputs = attrs.propagatedBuildInputs ++ [sphinx-copybutton];
  });

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
  ghcStaticDwarf = shared.ghcStatic.override { enableDwarf = true; };
  } else {})

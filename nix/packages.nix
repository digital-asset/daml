# The root file providing all definitions for the DA nix packages.
# They are explained below.

{ system ? builtins.currentSystem }:

let
  pkgs = import ./nixpkgs.nix { inherit system; };

  pkgs-1903 = import (import ./nixpkgs/nixos-19.03) {
    inherit system;
    config = {};
    overlays = [];
  };

  # Selects "bin" output from multi-output derivations which are has it. For
  # other multi-output derivations, select only the first output. For
  # single-output generation, do nothing.
  #
  # This ensures that as few output as possible of the tools we use below are
  # realized by Nix.
  selectBin = pkg:
    if pkg == null then
      null
    else if builtins.hasAttr "bin" pkg then
      pkg.bin
    else if builtins.hasAttr "outputs" pkg then
      builtins.getAttr (builtins.elemAt pkg.outputs 0) pkg
    else
      pkg;

  # Add all packages that are used by Bazel builds here
  bazel_dependencies = import ./bazel.nix { inherit system pkgs; };

in rec {
  inherit pkgs;
  ghc = bazel_dependencies.ghc;

  # GHC with the "c2hs" package included.
  ghcWithC2hs = bazel_dependencies.ghcWithC2hs;

  # Tools used in the dev-env. These are invoked through wrappers
  # in dev-env/bin. See the development guide for more information:
  # https://digitalasset.atlassian.net/wiki/spaces/DEL/pages/104431683/Maintaining+the+Nix+Development+Environment
  tools = pkgs.lib.mapAttrs (_: pkg: selectBin pkg) (rec {
    # Code generators

    make            = pkgs.gnumake;
    m4              = pkgs.m4;

    thrift          = pkgs.thrift;
    protoc          = bazel_dependencies.protobuf3_5;

    # Haskell development
    ghcWithC2hs     = bazel_dependencies.ghcWithC2hs;
    ghcid           = pkgs.haskellPackages.ghcid;
    hlint           = bazel_dependencies.hlint;
    ghci            = bazel_dependencies.ghc.ghc;

    # Hazel’s configure step currently searches for the C compiler in
    # PATH instead of taking it from our cc toolchain so we have to add
    # it to dev-env. See https://github.com/FormationAI/hazel/issues/80
    # for the upstream issue.
    cc = bazel_dependencies.bazel-cc-toolchain;

    # TLA+ with the command-line model checker TLC
    tlc2            = pkgs.tlaplus;

    mvn = bazel_dependencies.mvn;

    zinc = pkgs.callPackage ./tools/zinc {};

    jdk    = bazel_dependencies.jdk8;
    java   = jdk;
    javac  = jdk;
    jinfo  = jdk;
    jmap   = jdk;
    jstack = jdk;
    jar    = jdk;

    bazel-deps = pkgs.callPackage ./tools/bazel-deps {};
    # The package itself is called bazel-watcher. However, the executable is
    # called ibazel. We call the attribute ibazel so that the default dev-env
    # wrapper works.
    ibazel = pkgs.callPackage ./tools/bazel-watcher {};

    scala = (pkgs.scala.override { jre = jdk; }).overrideAttrs (attrs: {
      buildInputs = attrs.buildInputs ++ [ pkgs.makeWrapper ];
      installPhase = attrs.installPhase + ''
        wrapProgram $out/bin/scala    --add-flags "-nobootcp"
        wrapProgram $out/bin/scalac   --add-flags "-nobootcp"
        wrapProgram $out/bin/scaladoc --add-flags "-nobootcp"
        wrapProgram $out/bin/scalap   --add-flags "-nobootcp"
      '';
    });
    fsc      = scala;
    scalac   = scala;
    scaladoc = scala;
    scalap   = scala;

    coursier = pkgs.coursier;
    scalafmt = pkgs.scalafmt.override { jre = jdk; };
    dependency-check = (pkgs.callPackage ./tools/dependency-check { });

    gradle = pkgs.gradle;

    # Nix development
    cabal2nix = pkgs.cabal2nix;

    pypi2nix  = pkgs.pypi2nix.override { pythonPackages = pkgs.python36Packages; };

    # Web development
    node        = bazel_dependencies.nodejs;
    npm         = bazel_dependencies.nodejs;
    yarn        = (pkgs.yarn.override {
      nodejs = bazel_dependencies.nodejs;
    }).overrideAttrs (oldAttrs: rec {
      version = "1.12.3";
      src = pkgs.fetchzip {
        url = "https://github.com/yarnpkg/yarn/releases/download/v${version}/yarn-v${version}.tar.gz";
        sha256 = "0izn7lfvfw046qlxdgiiiyqj24sl2yclm6v8bzy8ilsr00csbrm2";
      };
    });

    node2nix  = pkgs.nodePackages.node2nix;

    live-server =
      (import ./tools/live-server { inherit pkgs; nodejs = tools.node; }).live-server;
    license-checker =
      (import ./tools/license-checker { inherit pkgs; nodejs = tools.node; }).license-checker;

    # This override is necessary to be able to run automated UI tests with Selenium 3.12.0
    # The override can be removed when nixpkgs snapshot moved past the commit of 6b91b0d09f582f308a8ad4de526df494ff363622
    chromedriver = pkgs.callPackage ./tools/chromedriver/default.nix {};

    # Python development
    pip3        = python36;
    python      = python36;
    python3     = python36;
    python36    = pkgs.python36Packages.python;

    ipython = pkgs.python36Packages.ipython;
    notebook = pkgs.python36Packages.notebook;
    numpy = pkgs.python36Packages.numpy;
    scipy = pkgs.python36Packages.scipy;
    matplotlib = pkgs.python36Packages.matplotlib;
    pandas = pkgs.python36Packages.pandas;
    cram = pkgs.callPackage ./python-modules/cram {};
    flake8 = pkgs.python36Packages.flake8;
    yapf = pkgs.python36Packages.yapf;

    # Pex packaging has been submitted upsteam as
    # https://github.com/NixOS/nixpkgs/pull/45497.
    # However, this one is for a newer version
    pex = pkgs.callPackage ./tools/pex {};
    # Pipenv packaging is taken from upstream commit:
    # https://github.com/NixOS/nixpkgs/commit/40887a6dc635.
    pipenv = pkgs.callPackage ./tools/pipenv {};

    # Databases
    cassandra = pkgs.cassandra;
    cqlsh     = cassandra;
    nodetool  = cassandra;

    sphinx            = pkgs.python36.withPackages (ps: [ps.sphinx ps.sphinx_rtd_theme]);
    sphinx-build      = sphinx;
    sphinx-quickstart = sphinx;

    sphinx-autobuild = import ./tools/sphinx-autobuild {
      inherit pkgs;
      pythonPackages = pkgs.python36Packages;
    };

    sphinx183 = bazel_dependencies.sphinx183;

    texlive   = bazel_dependencies.texlive;
    bibtex    = bazel_dependencies.texlive;
    latexmk   = bazel_dependencies.texlive;
    makeindex = bazel_dependencies.texlive;
    pdflatex  = bazel_dependencies.texlive;
    lualatex  = bazel_dependencies.texlive;

    convert = bazel_dependencies.imagemagick;

    # The sass derivation in nixos-18.09 is broken, so we add our own
    # created with bundix.
    sass = bazel_dependencies.sass;

    graphviz  = pkgs.graphviz_2_32;
    dot       = graphviz;
    tred      = graphviz;
    unflatten = graphviz;
    circo     = graphviz;

    # Build tools

    # wrap the .bazelrc to automate the configuration of
    # `build --config <kernel>`
    bazelrc =
      let
        kernel =
          if pkgs.stdenv.targetPlatform.isLinux then "linux"
          else if pkgs.stdenv.targetPlatform.isDarwin then "darwin"
          else throw "unsupported system";
      in
        pkgs.writeText "daml-bazelrc" ''
          build --config ${kernel}
        '';

    bazel = pkgs.writeScriptBin "bazel" (''
      if [ -z "''${DADE_REPO_ROOT:-}" ]; then
          >&2 echo "Please run bazel inside of the dev-env"
          exit 1
      fi
      export BAZEL_USE_CPP_ONLY_TOOLCHAIN=1
      # Set the JAVA_HOME to our JDK
      export JAVA_HOME=${jdk.home}
      export GIT_SSL_CAINFO="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
    '' + pkgs.stdenv.lib.optionalString (pkgs.buildPlatform.libc == "glibc") ''
      export LOCALE_ARCHIVE="${pkgs.glibcLocales}/lib/locale/locale-archive"
    '' + ''
      exec ${pkgs.bazel}/bin/bazel --bazelrc "${bazelrc}" "$@"
    '');

    # System tools
    shellcheck = pkgs.shellcheck;
    curl = bazel_dependencies.curl;

    patch = pkgs.patch;
    wget = pkgs.wget;
    grpcurl = pkgs-1903.grpcurl;

    # String mangling tooling.
    jo   = pkgs.jo;
    jq   = bazel_dependencies.jq;
    gawk = bazel_dependencies.gawk;
    sed = pkgs.gnused;
    base64 = pkgs.coreutils;
    sha1sum = pkgs.coreutils;
    xmlstarlet = pkgs.xmlstarlet;

    # Packaging tools
    patchelf = bazel_dependencies.patchelf;
    zip = bazel_dependencies.zip;
    openssl = pkgs.openssl.bin;
    tar = bazel_dependencies.gnutar;

    semver = pkgs.callPackage ./tools/semver-tool {};
    osht = pkgs.callPackage ./tools/osht {};
    bats = pkgs.callPackage ./tools/bats {};
    dade-test-sh = pkgs.callPackage ./tools/dade-test-sh {};

    undmg = pkgs.undmg;
    jfrog = pkgs.callPackage ./tools/jfrog-cli {};

    # Cloud tools
    gcloud = pkgs.google-cloud-sdk;
    bq     = gcloud;
    gsutil = gcloud;
    terraform = pkgs-1903.terraform.withPlugins (p: with p; [
      google
      google-beta
      random
      secret
      template
    ]);
    nix-store-gcs-proxy = pkgs.callPackage ./tools/nix-store-gcs-proxy {};
  });

  # Set of packages that we want Hydra to build for us
  cached = bazel_dependencies // {
    # Python packages used via 'python3.6-da'.
    pythonPackages = {
      inherit (pkgs.python36Packages)
        pyyaml semver GitPython;
    };
    # Packages used in command-line tools, e.g. `dade-info`.
    cli-tools = {
      inherit (pkgs) coreutils nix-info getopt;
    };
    # Used by CI
    minio  = pkgs-1903.minio;
  } // (if pkgs.stdenv.isLinux then {
    # The following packages are used for CI docker based builds
    bash = pkgs.bash;
    busybox = pkgs.busybox;
    bzip2 = pkgs.bzip2;
    cacert = pkgs.cacert;
    cheat = pkgs.cheat;
    coreutils = pkgs.coreutils;
    docker-compose  = pkgs.python36Packages.docker_compose;
    dockerd = pkgs.docker;
    findutils = pkgs.findutils;
    ftop = pkgs.ftop;
    gcc7 = pkgs.gcc7;
    glibc = pkgs.glibc;
    gnugrep = pkgs.gnugrep;
    iputils = pkgs.iputils;
    less = pkgs.less;
    ltrace = pkgs.ltrace;
    lvm2 = pkgs.lvm2;
    ncurses = pkgs.ncurses;
    nettools = pkgs.nettools;
    procps = pkgs.procps;
    glibcLocales = pkgs.glibcLocales;
    strace = pkgs.strace;
    sudo = pkgs.sudo;
    su = pkgs.su;
    tcpdump = pkgs.tcpdump;
    tldr = pkgs.tldr;
    tmux = pkgs.tmux;
    utillinux = pkgs.utillinux;
    vim = pkgs.vim;
    which = pkgs.which;
    zsh = pkgs.zsh;
    openssh = pkgs.openssh;
  }
  else {});

  # The build environment used for the 'da' package set above.
  # Exported here for testing purposes.
  environment = {
    ghc = bazel_dependencies.ghc;
    cabal2nix = tools.cabal2nix;
  };

  dade = {
    tools-list = pkgs.runCommand "tools-list" {
      ts = builtins.concatStringsSep " " (builtins.attrNames tools);
      preferLocalBuild = true;
    } "echo $ts > $out";
  };
}

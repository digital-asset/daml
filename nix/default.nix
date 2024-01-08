{ system ? import ./system.nix
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:

let
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

  toolAttrs = rec {
    # Code generators

    make            = pkgs.gnumake;
    m4              = pkgs.m4;

    thrift          = pkgs.thrift;
    protoc          = bazel_dependencies.protobuf3_8;

    # Haskell development
    ghc             = bazel_dependencies.ghc;
    # current nixpkgs version has broken ghcid on m1; trying to fix that breaks
    # all sorts of other things.
    ghcid           = (let
                         spec = builtins.fromJSON (builtins.readFile ./nixpkgs/ghcid.src.json);
                       in
                         import (builtins.fetchTarball {
                           url = "https://github.com/${spec.owner}/${spec.repo}/archive/${spec.rev}.tar.gz";
                           sha256 = spec.sha256;
                       }) {}).haskellPackages.ghcid;
    hlint           = bazel_dependencies.hlint;
    ghci            = bazel_dependencies.ghc;

    # Hazel’s configure step currently searches for the C compiler in
    # PATH instead of taking it from our cc toolchain so we have to add
    # it to dev-env. See https://github.com/FormationAI/hazel/issues/80
    # for the upstream issue.
    cc = bazel_dependencies.bazel-cc-toolchain;

    mvn = bazel_dependencies.mvn;

    zinc = pkgs.callPackage ./tools/zinc {};

    jdk    = bazel_dependencies.jdk11;
    java   = jdk;
    javac  = jdk;
    jinfo  = jdk;
    jmap   = jdk;
    jstack = jdk;
    jar    = jdk;

    javafmt = pkgs.callPackage ./tools/google-java-format {};

    buf = pkgs.buf;

    scala = bazel_dependencies.scala_2_13;
    fsc      = scala;
    scalac   = scala;
    scaladoc = scala;
    scalap   = scala;
    sbt      = pkgs.sbt;

    cs = pkgs.coursier;
    # nixpkgs ships with an RC for scalafmt 2.0 that seems to be significantly slower
    # and changes a lot of formatting so for now we stick to 1.5.1.
    scalafmt = pkgs.callPackage ./overrides/scalafmt.nix { jre = jdk; };

    # Nix development
    cabal2nix = pkgs.cabal2nix;

    pypi2nix  = pkgs.pypi2nix;

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

    chromedriver = pkgs.chromedriver;

    # Python development
    pip3        = pkgs.python38Packages.pip;
    python      = pkgs.python38Packages.python;
    python3     = python;

    yapf = pkgs.python38Packages.yapf;

    pex = pkgs.python38Packages.pex;
    pipenv = pkgs.pipenv;

    pre-commit = pkgs.pre-commit;

    convert = bazel_dependencies.imagemagick;

    sass = bazel_dependencies.sass;

    graphviz  = pkgs.graphviz;
    dot       = graphviz;
    tred      = graphviz;
    unflatten = graphviz;
    circo     = graphviz;

    pandoc = pkgs.pandoc;

    # Build tools

    bazel = pkgs.writeScriptBin "bazel" (''
      #!${pkgs.bash}/bin/bash
      # Set the JAVA_HOME to our JDK
      export JAVA_HOME=${jdk.home}
      export GIT_SSL_CAINFO="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
    '' + pkgs.lib.optionalString (pkgs.buildPlatform.libc == "glibc") ''
      export LOCALE_ARCHIVE="${pkgs.glibcLocales}/lib/locale/locale-archive"
    '' + ''
      exec ${pkgs.bazel_5}/bin/bazel --bazelrc "${bazelrc}" "$@"
    '');

    # System tools
    shellcheck = pkgs.shellcheck;
    curl = bazel_dependencies.curl;
    lsof = pkgs.lsof;
    diff = pkgs.diffutils;

    patch = pkgs.patch;
    timeout = pkgs.coreutils;
    wget = pkgs.wget;
    grpcurl = pkgs.grpcurl;

    # String mangling tooling.
    base64 = pkgs.coreutils;
    bc = pkgs.bc;
    date = pkgs.coreutils;
    find = pkgs.findutils;
    gawk = bazel_dependencies.gawk;
    grep = pkgs.gnugrep;
    jq = bazel_dependencies.jq;
    sed = pkgs.gnused;
    sha1sum = pkgs.coreutils;
    sha256sum = pkgs.coreutils;
    xargs = pkgs.findutils;
    xmlstarlet = pkgs.xmlstarlet;

    # Cryptography tooling
    gnupg = pkgs.gnupg;
    gpg   = gnupg;

    # Packaging tools
    patchelf = bazel_dependencies.patchelf;
    zip = bazel_dependencies.zip;
    unzip = pkgs.unzip;
    openssl = pkgs.openssl.bin;
    tar = bazel_dependencies.gnutar;

    semver = pkgs.callPackage ./tools/semver-tool {};

    undmg = pkgs.undmg;

    # Cloud tools
    aws = pkgs.awscli;
    az = pkgs.azure-cli;
    gcloud = pkgs.google-cloud-sdk;
    bq = gcloud;
    gsutil = gcloud;
    docker-credential-gcloud = gcloud;
    # used to set up the webide CI pipeline in azure-cron.yml
    docker-credential-gcr = pkgs.docker-credential-gcr;
    terraform = pkgs.terraform_1.withPlugins (p: with p; [
      azurerm
      google
      google-beta
      secret
      random
    ]);
    nix-store-gcs-proxy = pkgs.callPackage ./tools/nix-store-gcs-proxy {};
  };

  # Tools used in the dev-env. These are invoked through wrappers in dev-env/bin.
  tools = pkgs.lib.mapAttrs (_: pkg: selectBin pkg) toolAttrs;

  # Set of packages that we want Hydra to build for us
  cached = bazel_dependencies // {
    # Packages used in command-line tools
    cli-tools = {
      inherit (pkgs) coreutils nix-info getopt;
    };
  } // (if pkgs.stdenv.isLinux then {
    # The following packages are used for CI docker based builds
    bash = pkgs.bash;
    busybox = pkgs.busybox;
    bzip2 = pkgs.bzip2;
    cacert = pkgs.cacert;
    cheat = pkgs.cheat;
    coreutils = pkgs.coreutils;
    dockerd = pkgs.docker;
    ftop = pkgs.ftop;
    gcc7 = pkgs.gcc7;
    glibc = pkgs.glibc;
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

  # On CI we only cache a subset of cached since we only build a subset of targets.
  ci-cached =
    if pkgs.stdenv.isDarwin
    then builtins.removeAttrs cached ["texlive"]
    else cached;

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

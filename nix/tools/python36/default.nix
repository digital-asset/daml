{ pkgs, src }:
let python = pkgs.python36.override {
  packageOverrides = self: super: rec {
    # This override can be removed once pygraphviz is on v1.5
    pygraphviz = super.callPackage ./pygraphviz { };
    hypothesis = super.hypothesis.override { doCheck = false; };
  };
};
  onLinux = pkgs.lib.optionals pkgs.stdenv.isLinux;
in pkgs.lib.overrideDerivation python (drv: rec {
    # Build with pip
    configureFlags = builtins.filter (x: x != "--without-ensurepip") drv.configureFlags;

    # We replace ncurses6 with ncurses5 in buildInputs since
    # PEP-513 (https://www.python.org/dev/peps/pep-0513/)
    # specifies that manylinux1 systems have libpanelw.so.5
    # and libncursesw.so.5 available.
    buildInputs = if pkgs.stdenv.isDarwin
        then drv.buildInputs
        else builtins.filter (p: p.name != pkgs.ncurses6.name) drv.buildInputs ++ [pkgs.ncurses5];
    # We now propagate the ncurses change to CPPFLAGS and
    # LDFLAGS. Here, it would have been nice to simply copy
    # the following two lines from the original derivation:
    #
    # CPPFLAGS="${concatStringsSep " " (map (p: "-I${getDev p}/include") buildInputs)}";
    # LDFLAGS="${concatStringsSep " " (map (p: "-L${getLib p}/lib") buildInputs)}";
    #
    # However, the derivations in buildInputs somehow behave
    # differently than the corresponding derivations in pkgs
    # and we don't get the expected output:
    #
    # nix-repl> "${pkgs.lib.getLib pkgs.zlib}"
    # "/nix/store/r43dk927l97n78rff7hnvsq49f3szkg6-zlib-1.2.11"
    #
    # nix-repl> "${pkgs.lib.getLib (builtins.elemAt pkgs.python3.buildInputs 0)}"
    # "/nix/store/8bsyix63bn598h4vr6zkx9r5yhv8f13r-zlib-1.2.11-dev"
    #
    # The first path is what is found in the original
    # LDFLAGS, the second output is what we get when we map
    # over buildInputs. To workaround this, we simply
    # prepend the desired directories to the search path.
    LDFLAGS = if pkgs.stdenv.isDarwin
        then drv.LDFLAGS
        else "-L${pkgs.lib.getLib pkgs.ncurses5}/lib " + drv.LDFLAGS;
    CPPFLAGS = if pkgs.stdenv.isDarwin
        then drv.CPPFLAGS
        else "-I${pkgs.lib.getDev pkgs.ncurses5}/include " + drv.CPPFLAGS;

    # Make all the required PEP-513 manylinux1 libraries
    # available at runtime.
    propagatedBuildInputs = drv.propagatedBuildInputs ++ onLinux [
        pkgs.ncurses5
        pkgs.xorg.libX11
        pkgs.xorg.libXext
        pkgs.xorg.libXrender
        pkgs.xorg.libICE
        pkgs.xorg.libSM
        pkgs.libGL
        pkgs.glib
    ];

    # Link against all the PEP-513 manylinux1 libraries to
    # make them available to Python C extensions.
    NIX_LDFLAGS = pkgs.lib.concatStringsSep " " ([drv.NIX_LDFLAGS] ++ onLinux [
        "-lstdc++"
        "-lpanelw"
        "-lncursesw"
        "-lX11"
        "-lXext"
        "-lXrender"
        "-lICE"
        "-lSM"
        "-lGL"
        "-lgobject-2.0"
        "-lgthread-2.0"
        "-lglib-2.0"
    ]);
    postInstall = drv.postInstall + ''
      # custom sitecustomize for setting SSL_CERT_PATH
      cp ${./sitecustomize.py} $out/lib/python3.6/sitecustomize.py
    '' + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
      # revert the upstream patching so the Python
      # installation becomes manylinux1 compatible again
      rm $out/lib/python3.6/_manylinux.py
    '';
  })

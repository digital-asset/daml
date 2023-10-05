let
  system = builtins.currentSystem;
  src = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/9a82a9b5248919805a2400266ebd881d5783df2a.tar.gz";
    sha256 = "142x1zq3cjadgmvfv0paydlq268pfinllqpq2vl0vxwdiq2nr9iz";
  };
  pkgs = import src {
    inherit system;

    config.allowUnfree = true;
    config.allowBroken = true;
  };
  selectBin = pkg:
    if pkg == null then
      null
    else if builtins.hasAttr "bin" pkg then
      pkg.bin
    else if builtins.hasAttr "outputs" pkg then
      builtins.getAttr (builtins.elemAt pkg.outputs 0) pkg
    else
      pkg;
in rec {
  inherit pkgs;

  toolAttrs = rec {
    semver = pkgs.callPackage ./tools/semver-tool {};
  };
  tools = pkgs.lib.mapAttrs (_: pkg: selectBin pkg) toolAttrs;
}

{ system ? import ./system.nix
, pkgs ? import ./nixpkgs.nix { inherit system; }
}:

let
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

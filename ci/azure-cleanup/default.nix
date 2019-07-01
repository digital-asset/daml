{ system ? builtins.currentSystem }:

let
  pkgs = import (import ../../nix/nixpkgs) {
    inherit system;
    config = {};
    overlays = [];
  };

  azure-devops = pkgs.callPackage ./azure-devops.nix { };
in
pkgs.mkShell {
  buildInputs = [ azure-devops ];
}

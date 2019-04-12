{ system ? builtins.currentSystem }:

let
  pkgs = import (import ../../nix/nixpkgs/nixos-19.03) {
    inherit system;
    config = {};
    overlays = [];
  };

  azure-devops = pkgs.callPackage ./azure-devops.nix { };
in
pkgs.mkShell {
  buildInputs = [ azure-devops ];
}

# Pinned version of nixpkgs that we use for our development and deployment.

{ system ? import ./system.nix
, ...
}:

let
  # See ./nixpkgs/README.md for upgrade instructions.
  src = import ./nixpkgs;

  nixpkgs = import src {
    inherit system;

    config.allowUnfree = true;
    config.allowBroken = true;
  };
in
  nixpkgs

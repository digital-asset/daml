let
  pkgs = import <nixpkgs> {};
  stdenv = pkgs.stdenv;
in rec {
  stainlessEnv = stdenv.mkDerivation rec {
    name = "stainless-env";
    shellHook = ''
    alias cls=clear
    '';
    buildInputs = with pkgs; [
      stdenv
      sbt
      openjdk17
      z3
    ];
  };
}

let
  pkgs = import <nixpkgs> {};
  stdenv = pkgs.stdenv;
  pdflatex_setup = pkgs.texlive.combine { inherit (pkgs.texlive) scheme-small parskip cleveref etoolbox; };
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
      pdflatex_setup
    ];
  };
}

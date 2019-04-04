{ lib, bundlerApp }:

bundlerApp {
  pname = "sass";
  gemdir = ./.;
  exes = [ "sass" "sass-convert" "scss" ];
}

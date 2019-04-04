# generated using pypi2nix tool (version: 1.6.0)
#
# COMMAND:
#   pypi2nix -V 3.5 -r requirements.txt
#

{ pkgs, python, commonBuildInputs ? [], commonDoCheck ? false }:

self: {

  "cram" = python.mkDerivation {
    name = "cram-0.7";
    src = pkgs.fetchurl { url = "https://pypi.python.org/packages/38/85/5a8a3397b2ccb2ffa3ba871f76a4d72c16531e43d0e58fc89a0f2983adbd/cram-0.7.tar.gz"; sha256 = "7da7445af2ce15b90aad5ec4792f857cef5786d71f14377e9eb994d8b8337f2f"; };
    doCheck = commonDoCheck;
    buildInputs = commonBuildInputs;
    propagatedBuildInputs = [ ];
    meta = with pkgs.stdenv.lib; {
      homepage = "";
      license = licenses.gpl1;
      description = "A simple testing framework for command line applications";
    };
  };

}

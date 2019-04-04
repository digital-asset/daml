# generated using pypi2nix tool (version: 1.8.0)
# See more at: https://github.com/garbas/pypi2nix
#
# COMMAND:
#   pypi2nix -V 3.6 -r requirements.txt
#

{ pkgs
, pythonPackages
}:

let

  inherit (pkgs) makeWrapper;
  inherit (pkgs.stdenv.lib) fix' extends inNixShell;

  commonBuildInputs = [];
  commonDoCheck = false;

  python = {
    mkDerivation = pythonPackages.buildPythonPackage;
  };

  generated = self: {

    "PyYAML" = python.mkDerivation {
      name = "PyYAML-3.12";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/4a/85/db5a2df477072b2902b0eb892feb37d88ac635d36245a72a6a69b23b383a/PyYAML-3.12.tar.gz"; sha256 = "592766c6303207a20efc445587778322d7f73b161bd994f227adaa341ba212ab"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.mit;
        description = "YAML parser and emitter for Python";
      };
    };



    "argh" = python.mkDerivation {
      name = "argh-0.26.2";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/e3/75/1183b5d1663a66aebb2c184e0398724b624cecd4f4b679cb6e25de97ed15/argh-0.26.2.tar.gz"; sha256 = "e9535b8c84dc9571a48999094fda7f33e63c3f1b74f3e5f3ac0105a58405bb65"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.lgpl2;
        description = "An unobtrusive argparse wrapper with natural syntax";
      };
    };



    "livereload" = python.mkDerivation {
      name = "livereload-2.5.1";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/e9/2e/c4972828cf526a2e5f5571d647fb2740df68f17e8084a9a1092f4d209f4c/livereload-2.5.1.tar.gz"; sha256 = "422de10d7ea9467a1ba27cbaffa84c74b809d96fb1598d9de4b9b676adf35e2c"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [
      self."six"
      self."tornado"
    ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.bsdOriginal;
        description = "Python LiveReload is an awesome tool for web developers";
      };
    };



    "pathtools" = python.mkDerivation {
      name = "pathtools-0.1.2";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/e7/7f/470d6fcdf23f9f3518f6b0b76be9df16dcc8630ad409947f8be2eb0ed13a/pathtools-0.1.2.tar.gz"; sha256 = "7c35c5421a39bb82e58018febd90e3b6e5db34c5443aaaf742b3f33d4655f1c0"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.mit;
        description = "File system general utilities";
      };
    };



    "port-for" = python.mkDerivation {
      name = "port-for-0.3.1";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/ec/f1/e7d7a36b5f3e77fba587ae3ea4791512ffff74bc1d065d6185e463279bc4/port-for-0.3.1.tar.gz"; sha256 = "b16a84bb29c2954db44c29be38b17c659c9c27e33918dec16b90d375cc596f1c"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.mit;
        description = "Utility that helps with local TCP ports managment. It can find an unused TCP localhost port and remember the association.";
      };
    };



    "six" = python.mkDerivation {
      name = "six-1.11.0";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/16/d8/bc6316cf98419719bd59c91742194c111b6f2e85abac88e496adefaf7afe/six-1.11.0.tar.gz"; sha256 = "70e8a77beed4562e7f14fe23a786b54f6296e34344c23bc42f07b15018ff98e9"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.mit;
        description = "Python 2 and 3 compatibility utilities";
      };
    };



    "sphinx-autobuild" = python.mkDerivation {
      name = "sphinx-autobuild-0.7.1";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/41/21/d7407dd6258ca4f4dfe6b3edbd076702042c02bfcdd82b6f71cb58a359d2/sphinx-autobuild-0.7.1.tar.gz"; sha256 = "66388f81884666e3821edbe05dd53a0cfb68093873d17320d0610de8db28c74e"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [
      self."PyYAML"
      self."argh"
      self."livereload"
      self."pathtools"
      self."port-for"
      self."tornado"
      self."watchdog"
    ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.mit;
        description = "Watch a Sphinx directory and rebuild the documentation when a change is detected. Also includes a livereload enabled web server.";
      };
    };



    "tornado" = python.mkDerivation {
      name = "tornado-4.5.2";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/fa/14/52e2072197dd0e63589e875ebf5984c91a027121262aa08f71a49b958359/tornado-4.5.2.tar.gz"; sha256 = "1fb8e494cd46c674d86fac5885a3ff87b0e283937a47d74eb3c02a48c9e89ad0"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = "License :: OSI Approved :: Apache Software License";
        description = "Tornado is a Python web framework and asynchronous networking library, originally developed at FriendFeed.";
      };
    };



    "watchdog" = python.mkDerivation {
      name = "watchdog-0.8.3";
      src = pkgs.fetchurl { url = "https://pypi.python.org/packages/54/7d/c7c0ad1e32b9f132075967fc353a244eb2b375a3d2f5b0ce612fd96e107e/watchdog-0.8.3.tar.gz"; sha256 = "7e65882adb7746039b6f3876ee174952f8eaaa34491ba34333ddf1fe35de4162"; };
      doCheck = commonDoCheck;
      buildInputs = commonBuildInputs;
      propagatedBuildInputs = pkgs.lib.concat
        [ self."argh" self."PyYAML" self."pathtools" ]
        (if pkgs.stdenv.isDarwin
         then with pkgs.darwin; [ apple_sdk.frameworks.CoreServices cf-private ]
         else []);

      meta = with pkgs.stdenv.lib; {
        homepage = "";
        license = licenses.asl20;
        description = "Filesystem events monitoring";
      };
    };

  };

in (fix' generated).sphinx-autobuild


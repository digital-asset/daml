# generated using pypi2nix tool (version: 2.0.4)
# See more at: https://github.com/nix-community/pypi2nix
#
# COMMAND:
#   pypi2nix -V python37 -e sphinx-autobuild
#

{ pkgs ? import <nixpkgs> {},
  overrides ? ({ pkgs, python }: self: super: {})
}:

let

  inherit (pkgs) makeWrapper;
  inherit (pkgs.stdenv.lib) fix' extends inNixShell;

  pythonPackages =
  import "${toString pkgs.path}/pkgs/top-level/python-packages.nix" {
    inherit pkgs;
    inherit (pkgs) stdenv;
    python = pkgs.python37;
  };

  commonBuildInputs = [];
  commonDoCheck = false;

  withPackages = pkgs':
    let
      pkgs = builtins.removeAttrs pkgs' ["__unfix__"];
      interpreterWithPackages = selectPkgsFn: pythonPackages.buildPythonPackage {
        name = "python37-interpreter";
        buildInputs = [ makeWrapper ] ++ (selectPkgsFn pkgs);
        buildCommand = ''
          mkdir -p $out/bin
          ln -s ${pythonPackages.python.interpreter} \
              $out/bin/${pythonPackages.python.executable}
          for dep in ${builtins.concatStringsSep " "
              (selectPkgsFn pkgs)}; do
            if [ -d "$dep/bin" ]; then
              for prog in "$dep/bin/"*; do
                if [ -x "$prog" ] && [ -f "$prog" ]; then
                  ln -s $prog $out/bin/`basename $prog`
                fi
              done
            fi
          done
          for prog in "$out/bin/"*; do
            wrapProgram "$prog" --prefix PYTHONPATH : "$PYTHONPATH"
          done
          pushd $out/bin
          ln -s ${pythonPackages.python.executable} python
          ln -s ${pythonPackages.python.executable} \
              python3
          popd
        '';
        passthru.interpreter = pythonPackages.python;
      };

      interpreter = interpreterWithPackages builtins.attrValues;
    in {
      __old = pythonPackages;
      inherit interpreter;
      inherit interpreterWithPackages;
      mkDerivation = args: pythonPackages.buildPythonPackage (args // {
        nativeBuildInputs = (args.nativeBuildInputs or []) ++ args.buildInputs;
      });
      packages = pkgs;
      overrideDerivation = drv: f:
        pythonPackages.buildPythonPackage (
          drv.drvAttrs // f drv.drvAttrs // { meta = drv.meta; }
        );
      withPackages = pkgs'':
        withPackages (pkgs // pkgs'');
    };

  python = withPackages {};

  generated = self: {
    "argh" = python.mkDerivation {
      name = "argh-0.26.2";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/e3/75/1183b5d1663a66aebb2c184e0398724b624cecd4f4b679cb6e25de97ed15/argh-0.26.2.tar.gz";
        sha256 = "e9535b8c84dc9571a48999094fda7f33e63c3f1b74f3e5f3ac0105a58405bb65";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "http://github.com/neithere/argh/";
        license = licenses.lgpl2;
        description = "An unobtrusive argparse wrapper with natural syntax";
      };
    };

    "livereload" = python.mkDerivation {
      name = "livereload-2.6.2";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/b5/a2/a36af515c73fac8d596b5e0f1f9b38b83a9366facb3959dd8428727cc650/livereload-2.6.2.tar.gz";
        sha256 = "d1eddcb5c5eb8d2ca1fa1f750e580da624c0f7fcb734aa5780dc81b7dcbd89be";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [
        self."six"
        self."tornado"
      ];
      meta = with pkgs.stdenv.lib; {
        homepage = "https://github.com/lepture/python-livereload";
        license = licenses.bsdOriginal;
        description = "Python LiveReload is an awesome tool for web developers";
      };
    };

    "pathtools" = python.mkDerivation {
      name = "pathtools-0.1.2";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/e7/7f/470d6fcdf23f9f3518f6b0b76be9df16dcc8630ad409947f8be2eb0ed13a/pathtools-0.1.2.tar.gz";
        sha256 = "7c35c5421a39bb82e58018febd90e3b6e5db34c5443aaaf742b3f33d4655f1c0";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "http://github.com/gorakhargosh/pathtools";
        license = licenses.mit;
        description = "File system general utilities";
      };
    };

    "port-for" = python.mkDerivation {
      name = "port-for-0.3.1";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/ec/f1/e7d7a36b5f3e77fba587ae3ea4791512ffff74bc1d065d6185e463279bc4/port-for-0.3.1.tar.gz";
        sha256 = "b16a84bb29c2954db44c29be38b17c659c9c27e33918dec16b90d375cc596f1c";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "https://github.com/kmike/port-for/";
        license = licenses.mit;
        description = "Utility that helps with local TCP ports managment. It can find an unused TCP localhost port and remember the association.";
      };
    };

    "pyyaml" = python.mkDerivation {
      name = "pyyaml-5.3.1";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/64/c2/b80047c7ac2478f9501676c988a5411ed5572f35d1beff9cae07d321512c/PyYAML-5.3.1.tar.gz";
        sha256 = "b8eac752c5e14d3eca0e6dd9199cd627518cb5ec06add0de9d32baeee6fe645d";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "https://github.com/yaml/pyyaml";
        license = licenses.mit;
        description = "YAML parser and emitter for Python";
      };
    };

    "six" = python.mkDerivation {
      name = "six-1.15.0";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/6b/34/415834bfdafca3c5f451532e8a8d9ba89a21c9743a0c59fbd0205c7f9426/six-1.15.0.tar.gz";
        sha256 = "30639c035cdb23534cd4aa2dd52c3bf48f06e5f4a941509c8bafd8ce11080259";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "https://github.com/benjaminp/six";
        license = licenses.mit;
        description = "Python 2 and 3 compatibility utilities";
      };
    };

    "sphinx-autobuild" = python.mkDerivation {
      name = "sphinx-autobuild-0.7.1";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/41/21/d7407dd6258ca4f4dfe6b3edbd076702042c02bfcdd82b6f71cb58a359d2/sphinx-autobuild-0.7.1.tar.gz";
        sha256 = "66388f81884666e3821edbe05dd53a0cfb68093873d17320d0610de8db28c74e";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [
        self."argh"
        self."livereload"
        self."pathtools"
        self."port-for"
        self."pyyaml"
        self."tornado"
        self."watchdog"
      ];
      meta = with pkgs.stdenv.lib; {
        homepage = "https://github.com/GaretJax/sphinx-autobuild";
        license = licenses.mit;
        description = "Watch a Sphinx directory and rebuild the documentation when a change is detected. Also includes a livereload enabled web server.";
      };
    };

    "tornado" = python.mkDerivation {
      name = "tornado-6.0.4";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/95/84/119a46d494f008969bf0c775cb2c6b3579d3c4cc1bb1b41a022aa93ee242/tornado-6.0.4.tar.gz";
        sha256 = "0fe2d45ba43b00a41cd73f8be321a44936dc1aba233dee979f17a042b83eb6dc";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [ ];
      meta = with pkgs.stdenv.lib; {
        homepage = "http://www.tornadoweb.org/";
        license = licenses.asl20;
        description = "Tornado is a Python web framework and asynchronous networking library, originally developed at FriendFeed.";
      };
    };

    "watchdog" = python.mkDerivation {
      name = "watchdog-0.10.2";
      src = pkgs.fetchurl {
        url = "https://files.pythonhosted.org/packages/73/c3/ed6d992006837e011baca89476a4bbffb0a91602432f73bd4473816c76e2/watchdog-0.10.2.tar.gz";
        sha256 = "c560efb643faed5ef28784b2245cf8874f939569717a4a12826a173ac644456b";
};
      doCheck = commonDoCheck;
      format = "setuptools";
      buildInputs = commonBuildInputs ++ [ ];
      propagatedBuildInputs = [
        self."pathtools"
      ];
      meta = with pkgs.stdenv.lib; {
        homepage = "http://github.com/gorakhargosh/watchdog";
        license = licenses.asl20;
        description = "Filesystem events monitoring";
      };
    };
  };
  localOverridesFile = ./requirements_override.nix;
  localOverrides = import localOverridesFile { inherit pkgs python; };
  commonOverrides = [
        (let src = pkgs.fetchFromGitHub { owner = "nix-community"; repo = "pypi2nix-overrides"; rev = "90e891e83ffd9e55917c48d24624454620d112f0"; sha256 = "0cl1r3sxibgn1ks9xyf5n3rdawq4hlcw4n6xfhg3s1kknz54jp9y"; } ; in import "${src}/overrides.nix" { inherit pkgs python; })
  ];
  paramOverrides = [
    (overrides { inherit pkgs python; })
  ];
  allOverrides =
    (if (builtins.pathExists localOverridesFile)
     then [localOverrides] else [] ) ++ commonOverrides ++ paramOverrides;

in python.withPackages
   (fix' (pkgs.lib.fold
            extends
            generated
            allOverrides
         )
   )
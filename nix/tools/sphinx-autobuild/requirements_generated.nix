{ pkgs, python }:

self: {
  "certifi" = python.mkDerivation {
    name = "certifi-2016.2.28";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/5c/f8/f6c54727c74579c6bbe5926f5deb9677c5810a33e11da58d1a4e2d09d041/certifi-2016.2.28.tar.gz";
      md5 = "5d672aa766e1f773c75cfeccd02d3650";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "ISC";
      description = "Python package for providing Mozilla's CA Bundle.";
    };
  };
  "watchdog" = python.mkDerivation {
    name = "watchdog-0.8.3";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/54/7d/c7c0ad1e32b9f132075967fc353a244eb2b375a3d2f5b0ce612fd96e107e/watchdog-0.8.3.tar.gz";
      md5 = "bb16926bccc98eae2a04535e4512ddf1";
    };
    doCheck = false;
    propagatedBuildInputs = pkgs.lib.concat
      [ self."argh" self."PyYAML" self."pathtools" ]
      (if pkgs.stdenv.isDarwin
      then with pkgs.darwin; [ apple_sdk.frameworks.CoreServices cf-private ]
      else []);
    meta = {
      homepage = "";
      license = "Apache License 2.0";
      description = "Filesystem events monitoring";
    };
  };
  "singledispatch" = python.mkDerivation {
    name = "singledispatch-3.4.0.3";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/d9/e9/513ad8dc17210db12cb14f2d4d190d618fb87dd38814203ea71c87ba5b68/singledispatch-3.4.0.3.tar.gz";
      md5 = "af2fc6a3d6cc5a02d0bf54d909785fcb";
    };
    doCheck = false;
    propagatedBuildInputs = [ self."six" ];
    meta = {
      homepage = "";
      license = "MIT";
      description = "This library brings functools.singledispatch from Python 3.4 to Python 2.6-3.3.";
    };
  };
  "backports-abc" = python.mkDerivation {
    name = "backports-abc-0.4";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/f5/d0/1d02695c0dd4f0cf01a35c03087c22338a4f72e24e2865791ebdb7a45eac/backports_abc-0.4.tar.gz";
      md5 = "0b65a216ce9dc9c1a7e20a729dd7c05b";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "";
      description = "A backport of recent additions to the 'collections.abc' module.";
    };
  };
  "argh" = python.mkDerivation {
    name = "argh-0.26.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/e3/75/1183b5d1663a66aebb2c184e0398724b624cecd4f4b679cb6e25de97ed15/argh-0.26.2.tar.gz";
      md5 = "edda25f3f0164a963dd89c0e3c619973";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "GNU Lesser General Public License (LGPL), Version 3";
      description = "An unobtrusive argparse wrapper with natural syntax";
    };
  };
  "pathtools" = python.mkDerivation {
    name = "pathtools-0.1.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/e7/7f/470d6fcdf23f9f3518f6b0b76be9df16dcc8630ad409947f8be2eb0ed13a/pathtools-0.1.2.tar.gz";
      md5 = "9a1af5c605768ea5804b03b734ff0f82";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "MIT License";
      description = "File system general utilities";
    };
  };
  "tornado" = python.mkDerivation {
    name = "tornado-4.3";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/21/29/e64c97013e97d42d93b3d5997234a6f17455f3744847a7c16289289f8fa6/tornado-4.3.tar.gz";
      md5 = "d13a99dc0b60ba69f5f8ec1235e5b232";
    };
    doCheck = false;
    propagatedBuildInputs = [ self."certifi" self."singledispatch" self."backports-abc" self."backports.ssl-match-hostname" ];
    meta = {
      homepage = "";
      license = "http://www.apache.org/licenses/LICENSE-2.0";
      description = "Tornado is a Python web framework and asynchronous networking library, originally developed at FriendFeed.";
    };
  };
  "backports.ssl-match-hostname" = python.mkDerivation {
    name = "backports.ssl-match-hostname-3.5.0.1";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/76/21/2dc61178a2038a5cb35d14b61467c6ac632791ed05131dda72c20e7b9e23/backports.ssl_match_hostname-3.5.0.1.tar.gz";
      md5 = "c03fc5e2c7b3da46b81acf5cbacfe1e6";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "Python Software Foundation License";
      description = "The ssl.match_hostname() function from Python 3.5";
    };
  };
  "six" = python.mkDerivation {
    name = "six-1.10.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/b3/b2/238e2590826bfdd113244a40d9d3eb26918bd798fc187e2360a8367068db/six-1.10.0.tar.gz";
      md5 = "34eed507548117b2ab523ab14b2f8b55";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "MIT";
      description = "Python 2 and 3 compatibility utilities";
    };
  };
  "sphinx-autobuild" = python.mkDerivation {
    name = "sphinx-autobuild-0.6.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/85/cf/25b65781e6d2a4a89a431260daf1e0d53a81c52d27c98245481d46f3df2a/sphinx-autobuild-0.6.0.tar.gz";
      md5 = "7331bb8e14b537b871ab8e6e09ff4713";
    };
    doCheck = false;
    propagatedBuildInputs = [ self."PyYAML" self."port-for" self."livereload" self."tornado" self."pathtools" self."argh" self."watchdog" ];
    meta = {
      homepage = "";
      license = "MIT";
      description = "Watch a Sphinx directory and rebuild the documentation when a change is detected. Also includes a livereload enabled web server.";
    };
  };
  "port-for" = python.mkDerivation {
    name = "port-for-0.3.1";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/ec/f1/e7d7a36b5f3e77fba587ae3ea4791512ffff74bc1d065d6185e463279bc4/port-for-0.3.1.tar.gz";
      md5 = "e6f4c466ce82fc9e9e0cb8ddee26a4c7";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "MIT license";
      description = "Utility that helps with local TCP ports managment. It can find an unused TCP localhost port and remember the association.";
    };
  };
  "livereload" = python.mkDerivation {
    name = "livereload-2.4.1";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/d3/fb/fa04cd6a08cc42e1ac089220b6f42d124d01aeb0c70fbe169a73713ca636/livereload-2.4.1.tar.gz";
      md5 = "e79d3de78f11b459392f347f7bb20309";
    };
    doCheck = false;
    propagatedBuildInputs = [ self."tornado" self."six" ];
    meta = {
      homepage = "";
      license = "BSD";
      description = "Python LiveReload is an awesome tool for web developers";
    };
  };
  "PyYAML" = python.mkDerivation {
    name = "PyYAML-3.11";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/04/60/abfb3a665ee0569b60c89148b7187ddd8a36cb65e254fba945ae1315645d/PyYAML-3.11.zip";
      md5 = "89cbc92cda979042533b640b76e6e055";
    };
    doCheck = false;
    propagatedBuildInputs = [  ];
    meta = {
      homepage = "";
      license = "MIT";
      description = "YAML parser and emitter for Python";
    };
  };
}

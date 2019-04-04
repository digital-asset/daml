{ stdenv, pkgs }:

let
  allSpecs = {
    "x86_64-linux" = {
      system = "linux64";
      sha256 = "0ki91xm0rmx2la68f7xmpxw7y01847wikp9y48zkvdjah9lqz7ji";
    };

    "x86_64-darwin" = {
      system = "mac64";
      sha256 = "0ql42dn27pw7v8xqlxsnnjaxjy8xs2v5qjb1z5rjncsdc3iyi8vd";
    };
  };

  spec = allSpecs."${stdenv.system}"
    or (throw "missing chromedriver binary for ${stdenv.system}");

in
pkgs.chromedriver.overrideAttrs (old: rec {

    version = "2.38";
    name = "chromedriver-${version}";
    src = pkgs.fetchurl {
        url = "http://chromedriver.storage.googleapis.com/${version}/chromedriver_${spec.system}.zip";
        sha256 = spec.sha256;
      };
  })

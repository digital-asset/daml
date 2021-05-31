{ lib, stdenv, fetchFromGitHub }:

stdenv.mkDerivation rec {
  version = "7cd86658";
  name = "semver-tool-${version}";

  src = fetchFromGitHub {
    inherit name;
    owner = "fsaintjacques";
    repo = "semver-tool";
    rev = version;
    sha256 = "1v70dgp5yl4di90p8gzbj97zylgc9q971ds5g84id78c2fh3xh28";
  };

  phases = [ "unpackPhase" "installPhase" ];


  installPhase =
    ''
      mkdir -p $out/bin
      cp $src/src/semver $out/bin/
      patchShebangs $out/bin/semver
    '';

  meta = {
    homepage = https://github.com/fsaintjacques/semver-tool;
    description = "The semver shell utility";
    license = lib.licenses.gpl3;
    platforms = lib.platforms.all;
  };
}

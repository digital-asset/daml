{ stdenv, fetchFromGitHub, mktemp }:

stdenv.mkDerivation rec {
  version = "20161205";
  name = "osht-git${version}";

  src = fetchFromGitHub {
    owner  = "coryb";
    repo   = "osht";
    rev    = "2a286b9bfe737fc4ec4b0fa3d1a2492c5b07aa7b";
    sha256 = "0qh84ndnahw30r1xls7j5rf8bb9ld5iyh8di3ljppqba3psc99yq";
  };

  buildInputs = [ mktemp ];

  phases = [ "installPhase" ];

  installPhase = ''
    mkdir -p $out/bin
    cp ${src}/osht.sh $out/bin/osht
    chmod +x $out/bin/osht
  '';

  meta = {
    homepage = https://github.com/coryb/osht;
    description = "osht can be used to trivally test command line clients";
    platforms = stdenv.lib.platforms.all;
    license = stdenv.lib.licenses.asl20;
  };
}

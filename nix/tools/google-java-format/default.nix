{ stdenv, fetchurl, jdk11, pkgs }:

let version = "1.9"; in
stdenv.mkDerivation rec {
  buildInputs = [ pkgs.makeWrapper ];
  name = "google-java-format";
  dontUnpack = true;
  src = fetchurl {
    url = "https://github.com/google/${name}/releases/download/${name}-${version}/${name}-${version}-all-deps.jar";
    sha256 = "1d98720a5984de85a822aa32a378eeacd4d17480d31cba6e730caae313466b97";
  };
  installPhase = ''
    mkdir -pv $out/share/java $out/bin
    cp ${src} $out/share/java/${name}-${version}.jar
    makeWrapper ${jdk11}/bin/java $out/bin/javafmt --add-flags "-jar $out/share/java/${name}-${version}.jar"
  '';
}

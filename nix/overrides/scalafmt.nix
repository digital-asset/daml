{ stdenv, jdk, jre, coursier, makeWrapper }:

let
  baseName = "scalafmt";
  version = "2.7.5";
  deps = stdenv.mkDerivation {
    name = "${baseName}-deps-${version}";
    buildCommand = ''
      export COURSIER_CACHE=$(pwd)
      ${coursier}/bin/coursier fetch org.scalameta:scalafmt-cli_2.12:${version} > deps
      mkdir -p $out/share/java
      cp $(< deps) $out/share/java/
    '';
    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
    outputHash     = "1xvx9bd6lf9m1r5p05d37qnjlzny6xrbkh8m7z4q4rk7i1vl8xv0";
  };
in
stdenv.mkDerivation rec {
  name = "${baseName}-${version}";

  buildInputs = [ jdk makeWrapper deps ];

  doCheck = true;

  phases = [ "installPhase" "checkPhase" ];

  installPhase = ''
    makeWrapper ${jre}/bin/java $out/bin/${baseName} \
      --add-flags "-cp $CLASSPATH org.scalafmt.cli.Cli"
  '';

  checkPhase = ''
    $out/bin/${baseName} --version | grep -q "${version}"
  '';

  meta = with stdenv.lib; {
    description = "Opinionated code formatter for Scala";
    homepage = http://scalafmt.org;
    license = licenses.asl20;
    maintainers = [ maintainers.markus1189 ];
  };
}

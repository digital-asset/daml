{ fetchzip, runCommand, stdenv }:

let
  version = "3.0.1";
  dependencyCheck = fetchzip {
    url = "https://dl.bintray.com/jeremy-long/owasp/dependency-check-${version}-release.zip";
    sha256 = "0x2mbgh79clw8q9xr95g05qaf42nk7bcb03d31fvqapri6hh7qcz";
  };
in runCommand "dependency-check-${version}-wrapper" { src = dependencyCheck; } ''
  mkdir -p $out/bin
  cp $src/bin/dependency-check.sh $out/bin/dependency-check
  chmod +x $out/bin/dependency-check
''

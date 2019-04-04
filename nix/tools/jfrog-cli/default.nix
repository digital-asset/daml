{ fetchurl, runCommand, stdenv }:

let
  version = "1.23.1";

  macSrc = fetchurl {
    url = "https://api.bintray.com/content/jfrog/jfrog-cli-go/${version}/jfrog-cli-mac-386/jfrog?bt_package=jfrog-cli-mac-386";
    sha256 = "e806db876f89a715185e42ae42e2a26aa82b39b11c4cc5e5717f13b6de4e079d";
  };

  linuxSrc = fetchurl {
    url = "https://api.bintray.com/content/jfrog/jfrog-cli-go/${version}/jfrog-cli-linux-amd64/jfrog?bt_package=jfrog-cli-linux-amd64";
    sha256 = "ec44c731e51a25217f6e87986e32477dd262610e245083615a4434efa1b12543";
  };

in stdenv.mkDerivation {
  name = "jfrog-cli-${version}";

  src = if stdenv.isDarwin then macSrc else linuxSrc;

  unpackPhase = ":";

  doConfigure = false;
  doBuild = false;

  installPhase = ''
      runHook preInstall
      mkdir -p $out/bin
    '' +
    stdenv.lib.optionalString stdenv.isDarwin ''
      cp $src $out/bin/jfrog
    '' +
    stdenv.lib.optionalString stdenv.isLinux ''
      # hack, as patchelf fails for go binaries
      # https://github.com/NixOS/patchelf/issues/66
      # TODO: build jfrog from source
      cp $src $out/bin/jfrog.wrapped
      chmod +x $out/bin/jfrog.wrapped
      echo $(< $NIX_CC/nix-support/dynamic-linker) $out/bin/jfrog.wrapped \"\$@\" > $out/bin/jfrog
    '' + ''
      chmod +x $out/bin/jfrog
      runHook postInstall
    '';

  postInstallCheck = ''
    $out/bin/jfrog --help
  '';

  meta = with stdenv.lib; {
    description = "JFrog CLI";
  };
}

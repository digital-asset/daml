{ stdenv, fetchurl }:

let
  version = "0.30.0";
  arch = "x86_64";
  src = if stdenv.isLinux then fetchurl {
    url = "https://github.com/bufbuild/buf/releases/download/v${version}/buf-Linux-${arch}.tar.gz";
    sha256 = "0q0mhr5pbg00lvb5mrpd073wi8nrkk1snbiba6znca1p6kljn350";
  } else fetchurl {
    url = "https://github.com/bufbuild/buf/releases/download/v${version}/buf-Darwin-${arch}.tar.gz";
    sha256 = "1whcmmg6bv1yfldzzs27zjdzcqncjkfh814ykls1fw422hj93jzb";
  };
in
stdenv.mkDerivation {
  name = "buf-${version}";
  inherit version src;
  installPhase = ''
    cp -a . $out
  '';
}

{ stdenv, fetchurl }:

let version = "0.3.13"; in
stdenv.mkDerivation rec {
	name = "zinc-${version}";

	src = fetchurl {
			url = "http://downloads.typesafe.com/zinc/${version}/zinc-${version}.tgz";
			sha256 = "6ae329abb526afde4ee78480be1f2675310b067e3e143fbb02f429f6f816f996";
	};

	installPhase = ''
		mkdir -p $out/bin
		cp -r bin/* $out/bin
		cp -r lib $out
	'';
}


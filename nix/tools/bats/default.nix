{ stdenv, pkgs }:

pkgs.bats.overrideAttrs (old: rec {
  version = "1.1.0";
  name = "bats-${version}";

  src = pkgs.fetchurl {
    url = "https://github.com/bats-core/bats-core/archive/v${version}.tar.gz";
    sha256 = "10pa2x3kd1w0cqdwwyiivgygc3jhhlld28qiwq2wass6xn5qnpc5";
  };

  meta = with stdenv.lib; {
    homepage = https://github.com/bats-core/bats-core;
    description = "Bash Automated Testing System";
    license = licenses.mit;
    platforms = platforms.unix;
  };
})

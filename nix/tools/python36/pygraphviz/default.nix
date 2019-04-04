# From https://github.com/NixOS/nixpkgs/blob/release-18.03/pkgs/development/python-modules/pygraphviz/default.nix, adjusted for v1.5.
{ stdenv, buildPythonPackage, fetchPypi, graphviz
, pkgconfig, doctest-ignore-unicode, mock, nose }:

buildPythonPackage rec {
  pname = "pygraphviz";
  version = "1.5";

  src = fetchPypi {
    inherit pname version;
    extension = "zip";
    sha256 = "179i3mjprhn200gcj6jq7c4mdrzckyqlh1srz78hynnw0nijka2h";
  };

  buildInputs = [ doctest-ignore-unicode mock nose ];
  propagatedBuildInputs = [ graphviz pkgconfig ];

  # the tests are currently failing:
  # check status of pygraphviz/pygraphviz#129
  doCheck = false;

  meta = with stdenv.lib; {
    description = "Python interface to Graphviz graph drawing package";
    homepage = https://github.com/pygraphviz/pygraphviz;
    license = licenses.bsd3;
    maintainers = with maintainers; [ ];
  };
}
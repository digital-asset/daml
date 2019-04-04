{ mkDerivation, ansi-wl-pprint, async, base, bytestring, clock
, directory, doctest, foldl, hostname, managed, optional-args
, optparse-applicative, process, semigroups, stdenv, stm
, system-fileio, system-filepath, temporary, text, time
, transformers, unix, unix-compat
}:
mkDerivation {
  pname = "turtle";
  version = "1.3.6";
  sha256 = "0fr8p6rnk2lrsgbfh60jlqcjr0nxrh3ywxsj5d4psck0kgyhvg1m";
  libraryHaskellDepends = [
    ansi-wl-pprint async base bytestring clock directory foldl hostname
    managed optional-args optparse-applicative process semigroups stm
    system-fileio system-filepath temporary text time transformers unix
    unix-compat
  ];
  testHaskellDepends = [ base doctest system-filepath temporary ];
  description = "Shell programming, Haskell-style";
  license = stdenv.lib.licenses.bsd3;
}

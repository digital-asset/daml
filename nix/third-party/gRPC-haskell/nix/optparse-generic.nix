{ mkDerivation, base, bytestring, optparse-applicative, semigroups
, stdenv, system-filepath, text, time, transformers, void
}:
mkDerivation {
  pname = "optparse-generic";
  version = "1.2.1";
  sha256 = "1dk945dp98mwk1v4y0cky3z0ngmd29nbg6fbaaxnigcrgpbvkjml";
  libraryHaskellDepends = [
    base bytestring optparse-applicative semigroups system-filepath
    text time transformers void
  ];
  description = "Auto-generate a command-line parser for your datatype";
  license = stdenv.lib.licenses.bsd3;
}

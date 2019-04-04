{ mkDerivation, aeson, aeson-pretty, attoparsec, base
, base64-bytestring, binary, bytestring, cereal, containers
, deepseq, doctest, fetchgit, foldl, hashable, haskell-src
, insert-ordered-containers, lens, mtl, neat-interpolation
, optparse-generic, parsec, parsers, pretty, pretty-show
, proto3-wire, QuickCheck, range-set-list, safe, semigroups, stdenv
, swagger2, system-filepath, tasty, tasty-hunit, tasty-quickcheck
, text, transformers, turtle, vector
}:
mkDerivation {
  pname = "proto3-suite";
  version = "0.2.0.0";
  src = fetchgit {
    url = "https://github.com/awakesecurity/proto3-suite.git";
    sha256 = "1khix03a4hwaqc192s523rjlsk1iq923ndmrj5myh61fr1fpcbaq";
    rev = "c103a8c6d3c16515fe2e9ea7f932d54729db2f5f";
  };
  isLibrary = true;
  isExecutable = true;
  enableSeparateDataOutput = true;
  libraryHaskellDepends = [
    aeson aeson-pretty attoparsec base base64-bytestring binary
    bytestring cereal containers deepseq foldl hashable haskell-src
    insert-ordered-containers lens mtl neat-interpolation parsec
    parsers pretty pretty-show proto3-wire QuickCheck safe semigroups
    swagger2 system-filepath text transformers turtle vector
  ];
  executableHaskellDepends = [
    base containers optparse-generic proto3-wire range-set-list
    system-filepath text turtle
  ];
  testHaskellDepends = [
    aeson attoparsec base base64-bytestring bytestring cereal doctest
    pretty-show proto3-wire QuickCheck semigroups swagger2 tasty
    tasty-hunit tasty-quickcheck text transformers turtle vector
  ];
  description = "A low level library for writing out data in the Protocol Buffers wire format";
  license = stdenv.lib.licenses.asl20;
}

{ mkDerivation, base, bytestring, cereal, containers, deepseq
, doctest, fetchgit, hashable, QuickCheck, safe, stdenv, tasty
, tasty-hunit, tasty-quickcheck, text, unordered-containers
}:
mkDerivation {
  pname = "proto3-wire";
  version = "1.0.0";
  src = fetchgit {
    url = "https://github.com/awakenetworks/proto3-wire.git";
    sha256 = "0nlar9zwy7k47nm395h11ivmhhfhip5bhyazwa2gnffn5lhsyv3i";
    rev = "d492fa3034724b46f23fb2c73780c9dd7ecb4d04";
  };
  libraryHaskellDepends = [
    base bytestring cereal containers deepseq hashable QuickCheck safe
    text unordered-containers
  ];
  testHaskellDepends = [
    base bytestring cereal doctest QuickCheck tasty tasty-hunit
    tasty-quickcheck text
  ];
  description = "A low-level implementation of the Protocol Buffers (version 3) wire format";
  license = stdenv.lib.licenses.asl20;
}

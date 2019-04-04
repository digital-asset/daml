{ mkDerivation, base, bytestring, cereal, containers, deepseq
, doctest, fetchgit, hashable, QuickCheck, safe, stdenv, tasty
, tasty-hunit, tasty-quickcheck, text, unordered-containers
}:
mkDerivation {
  pname = "proto3-wire";
  version = "1.0.0";
  src = fetchgit {
    url = "https://github.com/awakenetworks/proto3-wire.git";
    sha256 = "14n0d16an782ayipirm5v2mvp58jgf65xvffqzp08p50sksil3gi";
    rev = "a938330bf794cf3fa05591d03906915df98d157c";
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

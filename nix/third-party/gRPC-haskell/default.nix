{ mkDerivation, async, base, bytestring, c2hs, clock, containers
, criterion, grpc-haskell-core, managed, pipes, proto3-suite
, proto3-wire, QuickCheck, random, safe, sorted-list, stdenv, stm
, tasty, tasty-hunit, tasty-quickcheck, text, time, transformers
, turtle, unix, vector
}:
mkDerivation {
  pname = "grpc-haskell";
  version = "0.0.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    async base bytestring clock containers grpc-haskell-core managed
    pipes proto3-suite proto3-wire safe sorted-list stm tasty
    tasty-hunit tasty-quickcheck transformers vector
  ];
  doCheck = false;
  libraryToolDepends = [ c2hs ];
  testHaskellDepends = [
    async base bytestring clock containers managed pipes proto3-suite
    QuickCheck safe tasty tasty-hunit tasty-quickcheck text time
    transformers turtle unix
  ];
  benchmarkHaskellDepends = [
    async base bytestring criterion proto3-suite random
  ];
  homepage = "https://github.com/awakenetworks/gRPC-haskell";
  description = "Haskell implementation of gRPC layered on shared C library";
  license = stdenv.lib.licenses.asl20;
}

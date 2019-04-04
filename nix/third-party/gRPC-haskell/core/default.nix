{ mkDerivation, async, base, bytestring, c2hs, clock, containers
, grpc, managed, pipes, proto3-suite, proto3-wire, QuickCheck, safe
, sorted-list, stdenv, stm, tasty, tasty-hunit, tasty-quickcheck
, text, time, transformers, turtle, unix, vector, openssl, c-ares
, zlib, libcxx, libcxxabi
}:
mkDerivation {
  pname = "grpc-haskell-core";
  version = "0.0.0.0";
  src = ./.;
  libraryHaskellDepends = [
    async base bytestring clock containers managed pipes proto3-suite
    proto3-wire safe sorted-list stm tasty tasty-hunit tasty-quickcheck
    transformers vector
  ];
  librarySystemDepends = [ grpc openssl c-ares zlib libcxx libcxxabi ];
  libraryToolDepends = [ c2hs ];
  testHaskellDepends = [
    async base bytestring clock containers managed pipes proto3-suite
    QuickCheck safe tasty tasty-hunit tasty-quickcheck text time
    transformers turtle unix
  ];
  homepage = "https://github.com/awakenetworks/gRPC-haskell";
  description = "Haskell implementation of gRPC layered on shared C library";
  license = stdenv.lib.licenses.asl20;
}

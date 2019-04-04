{ mkDerivation, alex, array, base, bytestring, containers, deepseq
, directory, filepath, happy, pretty, process, stdenv, syb
}:
mkDerivation {
  pname = "language-c";
  version = "0.8.2";
  sha256 = "b729d3b2263b0f029a66c37ae1c05b86b68bad1cde6c0b407bfd5201b91fce15";
  revision = "1";
  editedCabalFile = "1xg49j4bykgdm6l14m65wyz8r3s4v4dqc7a9zjcsr12ffkiv8nam";
  libraryHaskellDepends = [
    array base bytestring containers deepseq directory filepath pretty
    process syb
  ];
  libraryToolDepends = [ alex happy ];
  testHaskellDepends = [ base directory filepath process ];
  homepage = "http://visq.github.io/language-c/";
  description = "Analysis and generation of C code";
  license = stdenv.lib.licenses.bsd3;
}

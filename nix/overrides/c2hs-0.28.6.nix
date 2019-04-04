{ mkDerivation, array, base, bytestring, containers, directory
, dlist, filepath, HUnit, language-c, pretty, process, shelly
, stdenv, test-framework, test-framework-hunit, text, transformers
}:
mkDerivation {
  pname = "c2hs";
  version = "0.28.6";
  sha256 = "91dd121ac565009f2fc215c50f3365ed66705071a698a545e869041b5d7ff4da";
  isLibrary = false;
  isExecutable = true;
  enableSeparateDataOutput = true;
  executableHaskellDepends = [
    array base bytestring containers directory dlist filepath
    language-c pretty process
  ];
  testHaskellDepends = [
    base filepath HUnit shelly test-framework test-framework-hunit text
    transformers
  ];
  homepage = "https://github.com/haskell/c2hs";
  description = "C->Haskell FFI tool that gives some cross-language type safety";
  license = stdenv.lib.licenses.gpl2;
}

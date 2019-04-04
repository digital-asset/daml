{ mkDerivation, aeson, base, base-compat, hashable, lens
, QuickCheck, semigroupoids, semigroups, stdenv, tasty
, tasty-quickcheck, text, transformers, unordered-containers
}:
mkDerivation {
  pname = "insert-ordered-containers";
  version = "0.2.1.0";
  sha256 = "1612f455dw37da9g7bsd1s5kyi84mnr1ifnjw69892amyimi47fp";
  revision = "3";
  editedCabalFile = "6fdce987672b006226243aa17522b57ec7a9e1cab247802eddbdaa9dc5b06446";
  libraryHaskellDepends = [
    aeson base base-compat hashable lens semigroupoids semigroups text
    transformers unordered-containers
  ];
  testHaskellDepends = [
    aeson base base-compat hashable lens QuickCheck semigroupoids
    semigroups tasty tasty-quickcheck text transformers
    unordered-containers
  ];
  homepage = "https://github.com/phadej/insert-ordered-containers#readme";
  description = "Associative containers retating insertion order for traversals";
  license = stdenv.lib.licenses.bsd3;
}

{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -Wwarn #-}

module Main where

import           ArbitraryGeneratedTestTypes ()
import           Control.Applicative
import           Control.Exception
import qualified Data.ByteString             as B
import qualified Data.ByteString.Builder     as BB
import qualified Data.ByteString.Char8       as BC
import qualified Data.ByteString.Lazy        as BL
import           Data.Either                 (isRight)
import           Data.Int
import           Data.Maybe                  (fromJust)
import           Data.Monoid
import           Data.Proxy
import           Data.String
import qualified Data.Text.Lazy              as TL
import           Data.Word                   (Word64)
import           GHC.Exts                    (fromList)
import           Proto3.Suite
import           Proto3.Wire.Decode          (ParseError)
import qualified Proto3.Wire.Decode          as Decode
import           Proto3.Wire.Types           as P
--import qualified Test.DocTest
import           Test.QuickCheck             (Arbitrary, Property, arbitrary,
                                              counterexample, oneof)
import           Test.Tasty
import           Test.Tasty.HUnit            (Assertion, assertBool, testCase,
                                              (@=?), (@?=))
import           Test.Tasty.QuickCheck       (testProperty, (===))
import           TestCodeGen
import qualified TestProto                   as TP
import qualified System.Directory

main :: IO ()
main = do
  --System.Directory.setCurrentDirectory "libs-haskell/proto3-suite"
  System.Directory.setCurrentDirectory "nix/third-party/proto3-suite"
  defaultMain tests

tests :: TestTree
tests = testGroup "Tests"
  [
  -- NOTE(JM): Not easy to make this work with Buck as proto3-suite
  -- isn't a registered package.
  -- docTests

    qcProperties
  , encodeUnitTests
  , decodeUnitTests
  , parserUnitTests
  , dotProtoUnitTests
  , codeGenTests
  ]

--------------------------------------------------------------------------------
-- Doctests

{-
docTests :: TestTree
docTests = testCase "doctests" $ do
  putStrLn "Running all doctests..."
  Test.DocTest.doctest
    [ "-isrc"
    , "-itests"
    , "src/Proto3/Suite/DotProto/Internal.hs"
    , "tests/TestCodeGen.hs"
    ]
-}

--------------------------------------------------------------------------------
-- QuickCheck properties

type MsgProp a = a -> Property

qcProperties :: TestTree
qcProperties = testGroup "QuickCheck properties"
  [ qcPropDecEncId
  ]

-- | Verifies that @decode . encode = id@ for various message types
qcPropDecEncId :: TestTree
qcPropDecEncId = testGroup "Property: (decode . encode = id) for various message types"
  [ testProperty "Trivial"             (prop :: MsgProp TP.Trivial)
  , testProperty "MultipleFields"      (prop :: MsgProp TP.MultipleFields)
  , testProperty "WithEnum"            (prop :: MsgProp TP.WithEnum)
  , testProperty "WithNesting"         (prop :: MsgProp TP.WithNesting)
  , testProperty "WithRepetition"      (prop :: MsgProp TP.WithRepetition)
  , testProperty "WithFixed"           (prop :: MsgProp TP.WithFixed)
  , testProperty "WithBytes"           (prop :: MsgProp TP.WithBytes)
  , testProperty "AllPackedTypes"      (prop :: MsgProp TP.AllPackedTypes)
  , testProperty "SignedInts"          (prop :: MsgProp TP.SignedInts)
  , testProperty "WithNestingRepeated" (prop :: MsgProp TP.WithNestingRepeated)
  , deeplyNest prop 1000
  ]
  where
    prop :: (Message a, Arbitrary a, Eq a, Show a) => MsgProp a
    prop msg = msg === (dec . enc) msg
      where
        dec = either (error . ("error parsing: " <>) . show) id . fromByteString
        enc = BL.toStrict . toLazyByteString

    deeplyNest :: MsgProp TP.Wrapped -> Int -> TestTree
    deeplyNest pf 0 = testProperty "Deeply nested" pf
    deeplyNest pf n = deeplyNest (pf . TP.Wrapped . Just) (n-1)


--------------------------------------------------------------------------------
-- Encoding

encodeUnitTests :: TestTree
encodeUnitTests = testGroup "Encoder unit tests"
  [ encoderMatchesGoldens
  ]

-- TODO: We should consider generating the reference encodings
-- (test-files/make_reference_encodings.py) as a part of running the test suite
-- rather than having them in the repository.
encoderMatchesGoldens :: TestTree
encoderMatchesGoldens = testGroup "Encoder matches golden encodings"
  [ check "trivial.bin"               $ TP.Trivial 123
  , check "trivial_negative.bin"      $ TP.Trivial (-1)
  , check "multiple_fields.bin"       $ TP.MultipleFields 1.23 (-0.5) 123 1234567890 "Hello, world!" True
  , check "signedints.bin"            $ TP.SignedInts (-42) (-84)
  , check "with_nesting.bin"          $ TP.WithNesting $ Just $ TP.WithNesting_Nested "123abc" 123456 [] []
  , check "with_enum0.bin"            $ TP.WithEnum $ Enumerated $ Right $ TP.WithEnum_TestEnumENUM1
  , check "with_enum1.bin"            $ TP.WithEnum $ Enumerated $ Right $ TP.WithEnum_TestEnumENUM2
  , check "with_repetition.bin"       $ TP.WithRepetition [1..5]
  , check "with_bytes.bin"            $ TP.WithBytes (BC.pack "abc") (fromList $ map BC.pack ["abc","123"])
  , check "with_nesting_repeated.bin" $ TP.WithNestingRepeated
                                          [ TP.WithNestingRepeated_Nested "123abc" 123456 [1,2,3,4] [5,6,7,8]
                                          , TP.WithNestingRepeated_Nested "abc123" 654321 [0,9,8,7] [6,5,4,3]
                                          ]
  ]
  where
    check fp v = testCase fp $ do
      goldenEncoding <- BL.readFile (testFilesPfx <> fp)
      toLazyByteString v @?= goldenEncoding

--------------------------------------------------------------------------------
-- Decoding

decodeUnitTests :: TestTree
decodeUnitTests = testGroup "Decoder unit tests"
  [ decodeFromGoldens
  ]

decodeFromGoldens :: TestTree
decodeFromGoldens = testGroup "Decode golden encodings into key/value lists"
  [ check "trivial.bin"
  , check "trivial_negative.bin"
  , check "multiple_fields.bin"
  , check "signedints.bin"
  , check "with_nesting.bin"
  , check "with_enum0.bin"
  , check "with_enum1.bin"
  , check "with_repetition.bin"
  , check "with_bytes.bin"
  , check "with_nesting_repeated.bin"
  ]
  where
    check fp = testCase fp $ do
      kvs <- Decode.decodeWire <$> B.readFile (testFilesPfx <> fp)
      assertBool ("parsing " <> fp <> " into a key-value list succeeds") (isRight kvs)

--------------------------------------------------------------------------------
-- Parser

parserUnitTests :: TestTree
parserUnitTests = testGroup "Parser unit tests"
  [ parseFromGoldens
  ]

parseFromGoldens :: TestTree
parseFromGoldens = testGroup "Parse golden encodings"
  [ check "trivial.bin"               $ TP.Trivial 123
  , check "multiple_fields.bin"       $ TP.MultipleFields 1.23 (-0.5) 123 1234567890 "Hello, world!" True
  , check "signedints.bin"            $ TP.SignedInts (-42) (-84)
  , check "with_nesting.bin"          $ TP.WithNesting $ Just $ TP.WithNesting_Nested "123abc" 123456 [] []
  , check "with_enum0.bin"            $ TP.WithEnum $ Enumerated $ Right $ TP.WithEnum_TestEnumENUM1
  , check "with_enum1.bin"            $ TP.WithEnum $ Enumerated $ Right $ TP.WithEnum_TestEnumENUM2
  , check "with_repetition.bin"       $ TP.WithRepetition [1..5]
  , check "with_fixed.bin"            $ TP.WithFixed (Fixed 16) (Fixed (-123)) (Fixed 4096) (Fixed (-4096))
  , check "with_bytes.bin"            $ TP.WithBytes (BC.pack "abc") (fromList $ map BC.pack ["abc","123"])
  , check "with_packing.bin"          $ TP.WithPacking [1,2,3] [1,2,3]
  , check "all_packed_types.bin"      $ TP.AllPackedTypes
                                          [1,2,3]
                                          [1,2,3]
                                          [-1,-2,-3]
                                          [-1,-2,-3]
                                          (fromList $ map Fixed [1..3])
                                          (fromList $ map Fixed [1..3])
                                          [1.0,2.0]
                                          [1.0,-1.0]
                                          (fromList $ map Fixed [1,2,3])
                                          (fromList $ map Fixed [1,2,3])
                                          [False,True]
                                          (Enumerated . Right <$> [TP.EFLD0, TP.EFLD1])
                                          (Enumerated . Right <$> [TP.EFLD0, TP.EFLD1])
  , check "with_nesting_repeated.bin" $ TP.WithNestingRepeated
                                          [ TP.WithNestingRepeated_Nested "123abc" 123456 [1,2,3,4] [5,6,7,8]
                                          , TP.WithNestingRepeated_Nested "abc123" 654321 [0,9,8,7] [6,5,4,3]
                                          ]
  , -- Checks parsing repeated embedded messages when one is expected (i.e.,
    -- this tests correct merging; this value was encoded as a
    -- WithNestingRepeated).
    check "with_nesting_repeated.bin" $ TP.WithNesting $ Just $ TP.WithNesting_Nested "abc123" 654321 [1,2,3,4,0,9,8,7] [5,6,7,8,6,5,4,3]
  , -- Checks that embedded message merging works correctly when fields have
    -- default values; this value was encoded as a WithNestingRepeatedInts
    check "with_nesting_ints.bin"     $ TP.WithNestingInts $ Just $ TP.NestedInts 2 2
  ]
  where
    check fp = testCase fp . testParser (testFilesPfx <> fp) fromByteString

testParser :: (Show a, Eq a)
           => FilePath -> (B.ByteString -> Either ParseError a) -> a -> IO ()
testParser fp p reference = do
  bs <- B.readFile fp
  case p bs of
    Left err        -> error $ "Got error: " ++ show err
    Right ourResult -> ourResult @?= reference

testDotProtoParse :: FilePath -> DotProto -> Assertion
testDotProtoParse file ast = do
  contents <- readFile file
  case parseProto (Path []) contents of
    Left err     -> error $ show err
    Right result -> ast @=? result

testDotProtoPrint :: DotProto -> String -> Assertion
testDotProtoPrint ast expected = expected @=? toProtoFileDef ast

testDotProtoRoundtrip :: DotProto -> Assertion
testDotProtoRoundtrip ast =
  Right ast @=? parseProto (Path []) (toProtoFileDef ast)

dotProtoUnitTests :: TestTree
dotProtoUnitTests = testGroup ".proto parsing tests"
  [ dotProtoParseTrivial
  , dotProtoPrintTrivial
  , dotProtoRoundtripTrivial
  , dotProtoRoundtripSimpleMessage
  , qcDotProtoRoundtrip
  ]

trivialDotProto :: DotProto
trivialDotProto = DotProto [] [] DotProtoNoPackage [] (DotProtoMeta (Path []))

dotProtoParseTrivial :: TestTree
dotProtoParseTrivial = testCase
  "Parse a content-less file" $
  testDotProtoParse "test-files/trivial.proto" trivialDotProto

dotProtoPrintTrivial :: TestTree
dotProtoPrintTrivial = testCase
  "Print a content-less DotProto" $
  testDotProtoPrint trivialDotProto "syntax = \"proto3\";"

dotProtoRoundtripTrivial :: TestTree
dotProtoRoundtripTrivial = testCase
  "Printing then parsing a content-less DotProto yields an empty DotProto" $
  testDotProtoRoundtrip trivialDotProto

dotProtoSimpleMessage :: DotProto
dotProtoSimpleMessage = DotProto [] [] DotProtoNoPackage
  [ DotProtoMessage (Single "MessageTest")
      [ DotProtoMessageField $
          DotProtoField (fieldNumber 1) (Prim Int32) (Single "testfield") [] Nothing
      ]
  ]
  (DotProtoMeta (Path []))

dotProtoRoundtripSimpleMessage :: TestTree
dotProtoRoundtripSimpleMessage = testCase
  "Round-trip for a single flat message" $
  testDotProtoRoundtrip dotProtoSimpleMessage

qcDotProtoRoundtrip :: TestTree
qcDotProtoRoundtrip = testProperty
  "Round-trip for a randomly-generated .proto AST" roundtrip
  where
    roundtrip :: DotProto -> Property
    roundtrip ast = let generated = toProtoFileDef ast
                    in case parseProto (Path []) generated of
                      Left err     -> error $ formatParseError err generated
                      Right result -> counterexample (formatMismatch ast generated result ) (ast == result)

    formatMismatch initial generated result = "AST changed during reparsing\n\nInitial AST:\n\n"
                                           ++ show initial
                                           ++ "\n\nGenerated .proto file:\n\n"
                                           ++ generated
                                           ++ "\n\nReparsed AST:\n\n"
                                           ++ show result
                                           ++ "\n\nRegenerated .proto file:\n\n"
                                           ++ (toProtoFileDef result)
    formatParseError err generated = "Parsec error:\n\n"
                                  ++ show err
                                  ++ "\n\nWhen attempting to parse:\n\n"
                                  ++ generated
                                  ++ "\n\nInitial AST:\n\n"

--------------------------------------------------------------------------------
-- Helpers

dotProtoFor :: (Named a, Message a) => Proxy a -> DotProto
dotProtoFor proxy = DotProto [] [] DotProtoNoPackage
  [ DotProtoMessage (Single (nameOf proxy)) (DotProtoMessageField <$> dotProto proxy)
  ]
  (DotProtoMeta (Path []))

showDotProtoFor :: (Named a, Message a) => Proxy a -> IO ()
showDotProtoFor = putStrLn . toProtoFileDef . dotProtoFor

instance Arbitrary WireType where
  arbitrary = oneof $ map return [Varint, P.Fixed32, P.Fixed64, LengthDelimited]

testFilesPfx :: IsString a => a
testFilesPfx = "test-files/"

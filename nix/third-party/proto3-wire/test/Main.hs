{-
  Copyright 2016 Awake Networks

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-}

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main where

import qualified Data.ByteString       as B
import qualified Data.ByteString.Lazy  as BL
import qualified Data.ByteString.Builder.Internal as BBI
import           Data.Either           ( isLeft )
import           Data.Maybe            ( fromMaybe )
import           Data.Monoid           ( (<>) )
import           Data.Int
import qualified Data.Text.Lazy        as T

import           Proto3.Wire
import qualified Proto3.Wire.Builder   as Builder
import qualified Proto3.Wire.Encode    as Encode
import qualified Proto3.Wire.Decode    as Decode

--import qualified Test.DocTest
import           Test.QuickCheck       ( (===), Arbitrary )
import           Test.Tasty
-- import           Test.Tasty.HUnit      ( (@=?) )
import qualified Test.Tasty.HUnit      as HU
import qualified Test.Tasty.QuickCheck as QC

-- import qualified System.Directory

main :: IO ()
main = do
    {-Test.DocTest.doctest
      [ "-isrc"
      , "src/Proto3/Wire/Builder.hs"
      , "src/Proto3/Wire/Encode.hs"
      , "src/Proto3/Wire/Decode.hs"
      ]-}
    defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [ roundTripTests
                          , buildSingleChunk
                          , decodeNonsense
                          ]

data StringOrInt64 = TString T.Text | TInt64 Int64
    deriving (Show,Eq)

instance QC.Arbitrary StringOrInt64 where
    arbitrary = QC.oneof [ TString . T.pack <$> QC.arbitrary, TInt64 <$> QC.arbitrary ]

roundTripTests :: TestTree
roundTripTests = testGroup "Roundtrip tests"
                           [ roundTrip "int32"
                                       (Encode.int32 (fieldNumber 1))
                                       (one Decode.int32 0 `at` fieldNumber 1)
                           , roundTrip "int64"
                                       (Encode.int64 (fieldNumber 1))
                                       (one Decode.int64 0 `at` fieldNumber 1)
                           , roundTrip "sint32"
                                       (Encode.sint32 (fieldNumber 1))
                                       (one Decode.sint32 0 `at` fieldNumber 1)
                           , roundTrip "sint64"
                                       (Encode.sint64 (fieldNumber 1))
                                       (one Decode.sint64 0 `at` fieldNumber 1)
                           , roundTrip "uint32"
                                       (Encode.uint32 (fieldNumber 1))
                                       (one Decode.uint32 0 `at` fieldNumber 1)
                           , roundTrip "uint64"
                                       (Encode.uint64 (fieldNumber 1))
                                       (one Decode.uint64 0 `at` fieldNumber 1)
                           , roundTrip "fixed32"
                                       (Encode.fixed32 (fieldNumber 1))
                                       (one Decode.fixed32 0 `at` fieldNumber 1)
                           , roundTrip "fixed64"
                                       (Encode.fixed64 (fieldNumber 1))
                                       (one Decode.fixed64 0 `at` fieldNumber 1)
                           , roundTrip "sfixed32"
                                       (Encode.sfixed32 (fieldNumber 1))
                                       (one Decode.sfixed32 0 `at` fieldNumber 1)
                           , roundTrip "sfixed64"
                                       (Encode.sfixed64 (fieldNumber 1))
                                       (one Decode.sfixed64 0 `at` fieldNumber 1)
                           , roundTrip "float"
                                       (Encode.float (fieldNumber 1))
                                       (one Decode.float 0 `at` fieldNumber 1)
                           , roundTrip "double"
                                       (Encode.double (fieldNumber 1))
                                       (one Decode.double 0 `at` fieldNumber 1)
                           , roundTrip "bool"
                                       (Encode.enum (fieldNumber 1))
                                       (one Decode.bool False `at` fieldNumber 1)
                           , roundTrip "text"
                                       (Encode.text (fieldNumber 1) . T.pack)
                                       (one (fmap T.unpack Decode.text) mempty `at`
                                            fieldNumber 1)
                           , roundTrip "embedded"
                                       (Encode.embedded (fieldNumber 1) .
                                            Encode.int32 (fieldNumber 1))
                                       (fmap (fromMaybe 0)
                                             (Decode.embedded (one Decode.int32
                                                                   0 `at`
                                                                   fieldNumber 1))
                                            `at` fieldNumber 1)
                           , roundTrip "multiple fields"
                                       (\(a, b) -> Encode.int32 (fieldNumber 1)
                                                                a <>
                                            Encode.uint32 (fieldNumber 2) b)
                                       ((,) <$>
                                            one Decode.int32 0 `at`
                                                fieldNumber 1
                                            <*> one Decode.uint32 0 `at`
                                                fieldNumber 2)
                           , roundTrip "oneof"
                                        (\case Just (TString text) -> Encode.text (fieldNumber 3) text
                                               Just (TInt64 i)     -> Encode.int64 (fieldNumber 2) i
                                               Nothing             -> mempty
                                        )
                                        (oneof Nothing
                                               [ (fieldNumber 2, Just . TInt64  <$> one Decode.int64 0)
                                               , (fieldNumber 3, Just . TString <$> one Decode.text mempty)
                                               ]
                                        )
                           , roundTrip "oneof-last"
                                        (\case Just (TString text) -> Encode.text (fieldNumber 3) "something" <> Encode.text (fieldNumber 3) text
                                               Just (TInt64 i)     -> Encode.int64 (fieldNumber 2) 20000000 <> Encode.int64 (fieldNumber 2) i
                                               Nothing             -> mempty
                                        )
                                        (oneof Nothing
                                               [ (fieldNumber 2, Just . TInt64  <$> one Decode.int64 0)
                                               , (fieldNumber 3, Just . TString <$> one Decode.text mempty)
                                               ]
                                        )

                           ]

roundTrip :: (Show a, Eq a, Arbitrary a)
          => String
          -> (a -> Encode.MessageBuilder)
          -> Decode.Parser Decode.RawMessage a
          -> TestTree
roundTrip name encode decode =
    QC.testProperty name $
        \x -> do
            let bytes = Encode.toLazyByteString (encode x)
            case Decode.parse decode (BL.toStrict bytes) of
                Left _ -> error "Could not decode encoded message"
                Right x' -> x === x'

buildSingleChunk :: TestTree
buildSingleChunk = HU.testCase "Builder creates a single chunk" $ do
  let chunks = length . BL.toChunks . Builder.toLazyByteString

      huge = B.replicate (BBI.maximalCopySize + 16) 1
      huge2 = Builder.byteString huge <> Builder.byteString huge

      hugeL = BL.fromChunks [huge, huge]
      hugeL2 = Builder.lazyByteString hugeL <> Builder.lazyByteString hugeL

  HU.assertBool "single chunk (strict)" $ chunks huge2 == 1
  HU.assertBool "single chunk (lazy)" $ chunks hugeL2 == 1

decodeNonsense :: TestTree
decodeNonsense = HU.testCase "Decoding a nonsensical string fails." $ do
  let decoded = Decode.parse (one Decode.fixed64 0 `at` fieldNumber 1) "test"
  HU.assertBool "decode fails" $ isLeft decoded

-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LFConversion.Tests (main) where

import Development.IDE.Types.Diagnostics (FileDiagnostic, showDiagnostics)
import Development.IDE.Types.Location (toNormalizedFilePath')
import Test.Tasty
import Test.Tasty.HUnit
import Data.Either.Combinators (whenLeft, whenRight)
import Data.Maybe (isNothing)
import Data.Ratio
import qualified Data.Text as T

import DA.Daml.LFConversion
import DA.Daml.LFConversion.MetadataEncoding
import qualified DA.Daml.LF.Ast as LF

import qualified "ghc-lib-parser" BasicTypes as GHC
import qualified "ghc-lib-parser" BooleanFormula as BF
import "ghc-lib-parser" SrcLoc (noLoc)

main :: IO ()
main = defaultMain $
  testGroup "LFConversion"
    [ metadataEncodingTests
    , bignumericTests
    ]

metadataEncodingTests :: TestTree
metadataEncodingTests = testGroup "MetadataEncoding"
    [ roundtripTests "functional dependencies" encodeFunDeps decodeFunDeps
        [ ("()", [])
        , ("(->)", [([], [])])
        , ("(a -> b)", [([LF.TypeVarName "a"], [LF.TypeVarName "b"])])
        , ("(a b -> c, c -> b a)"
          , [([LF.TypeVarName "a", LF.TypeVarName "b"], [LF.TypeVarName "c"])
            ,([LF.TypeVarName "c"], [LF.TypeVarName "b", LF.TypeVarName "a"])]
          )
        ]
    , roundtripTests "boolean formulas" encodeBooleanFormula decodeBooleanFormula
        [ ("true", BF.mkTrue)
        , ("false", BF.mkFalse)
        , ("a", BF.Var "a")
        , ("and [a]", BF.And [noLoc (BF.Var "a")])
        , ("or [a]", BF.Or [noLoc (BF.Var "a")])
        , ("parens a", BF.Parens (noLoc (BF.Var "a")))
        , ("and [or [a, b], c]"
          , BF.And
                [ noLoc (BF.Or [noLoc (BF.Var "a")
                , noLoc (BF.Var "b")]), noLoc (BF.Var "c")]
          )
        ]
    , roundtripTestsPartial "overlap modes" encodeOverlapMode decodeOverlapMode
        [ ("no overlap", GHC.NoOverlap GHC.NoSourceText) ]
        [ ("overlappable", GHC.Overlappable GHC.NoSourceText)
        , ("overlapping", GHC.Overlapping GHC.NoSourceText)
        , ("overlaps", GHC.Overlaps GHC.NoSourceText)
        , ("incoherent", GHC.Incoherent GHC.NoSourceText)
        ]
    ]

roundtripTests :: (Eq a) => String -> (a -> b) -> (b -> Maybe a) -> [(String, a)] -> TestTree
roundtripTests groupName encode decode examples =
    roundtripTestsPartial groupName (Just . encode) decode [] examples

roundtripTestsPartial :: (Eq a) => String -> (a -> Maybe b) -> (b -> Maybe a) -> [(String, a)] -> [(String, a)] -> TestTree
roundtripTestsPartial groupName encode decode negativeExamples positiveExamples =
    testGroup groupName $
        [ testCase name $
            assertBool "expected value to not have encoding"
                (isNothing (encode value))
        | (name, value) <- negativeExamples
        ] ++
        [ testCase name $
            assertBool "expected value to survive roundtrip"
                (Just value == (encode value >>= decode))
        | (name, value) <- positiveExamples
        ]

bignumericTests :: TestTree
bignumericTests = testGroup "BigNumeric"
  [ valid 0.123457
  , valid 0.123456
  , valid 0.0
  , invalid 123456789012345678901233456789012345678
  , invalid (-123456789012345678901233456789012345678)
  ]
  where
    valid :: Rational -> TestTree
    valid r = testCase (show r <> " should be valid") $ do
      let result = convertLiteral r
      whenLeft result $ \diag ->
        assertFailure $ T.unpack $ showDiagnostics [diag]
    invalid :: Rational -> TestTree
    invalid r = testCase (show r <> " should be invalid") $ do
      let result = convertLiteral r
      whenRight result $ \_ ->
        assertFailure $ "Expected " <> show r <> " to be an invalid BigNumeric literal but conversion succeeeded"
    convertLiteral :: Rational -> Either FileDiagnostic LF.Expr
    convertLiteral r =
        let dummyEnv = ConversionEnv
              { convModuleFilePath = toNormalizedFilePath' ""
              , convRange = Nothing
              }
        in runConvertM dummyEnv $ convertRationalBigNumeric (numerator r) (denominator r)

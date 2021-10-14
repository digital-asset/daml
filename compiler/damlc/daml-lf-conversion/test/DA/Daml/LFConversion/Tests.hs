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
import qualified Data.Set as S
import qualified Data.Text as T

import DA.Daml.LFConversion
import DA.Daml.LFConversion.MetadataEncoding
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.UtilGHC (fsFromText)

import qualified "ghc-lib-parser" BasicTypes as GHC
import qualified "ghc-lib-parser" BooleanFormula as BF
import qualified "ghc-lib-parser" FieldLabel as GHC
import "ghc-lib-parser" OccName (mkVarOcc, mkTcOcc)
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
    , roundtripTests "module imports" encodeModuleImports decodeModuleImports
        [ ("()", S.empty)
        , ("(Foo.Bar)"
          , S.fromList
            [ mkImport Nothing ["Foo", "Bar"]])
        , ("(\"foo\" Foo.Bar)"
          , S.fromList
            [ mkImport (Just "foo") ["Foo", "Bar"]])
        , ("(Foo.Bar, Baz.Qux.Florp)"
          , S.fromList
            [ mkImport Nothing ["Foo", "Bar"]
            , mkImport Nothing ["Baz", "Qux", "Florp"]])
        , ("(\"foo\" Foo.Bar, \"baz\" Baz.Qux.Florp)"
          , S.fromList
            [ mkImport (Just "foo") ["Foo", "Bar"]
            , mkImport (Just "baz") ["Baz", "Qux", "Florp"]])
        ]
    , roundtripTests "exports" encodeExports decodeExports
        [ ("()", [])
        , ("(Foo.Bar (qux))"
          , [ mkExportInfoVal Nothing ["Foo", "Bar"] "qux" ])
        , ("(\"foo\" Foo.Bar (qux))"
          , [ mkExportInfoVal (Just "foo") ["Foo", "Bar"] "qux" ])
        , ("(Foo.Bar (Qux()))"
          , [ mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" [] []])
        , ("(\"foo\" Foo.Bar (Qux()))"
          , [ mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" [] []])
        , ("(Foo.Bar (Qux(getQux)))"
          , [ mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" ["getQux"] []])
        , ("(\"foo\" Foo.Bar (Qux(getQux)))"
          , [ mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" ["getQux"] []])
        , ("(Foo.Bar (Qux($sel:getQux:Qux)))"
          , [ mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" [] ["getQux"]])
        , ("(\"foo\" Foo.Bar (Qux($sel:getQux:Qux)))"
          , [ mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" [] ["getQux"]])
        ]
    ]

mkImport :: Maybe T.Text -> [T.Text] -> LF.Qualified ()
mkImport mPackage moduleComponents =
  LF.Qualified
    { qualPackage = maybe LF.PRSelf (LF.PRImport . LF.PackageId) mPackage
    , qualModule = LF.ModuleName moduleComponents
    , qualObject = ()
    }

mkExportInfoVal :: Maybe T.Text -> [T.Text] -> T.Text -> ExportInfo
mkExportInfoVal mPackage moduleComponents value =
  ExportInfoVal $ QualName (mkImport mPackage moduleComponents)
    { LF.qualObject = mkVarOcc (T.unpack value)
    }

mkExportInfoTC :: Maybe T.Text -> [T.Text] -> T.Text -> [T.Text] -> [T.Text] -> ExportInfo
mkExportInfoTC mPackage moduleComponents value pieces fields =
  ExportInfoTC name (name:pieceNames) fieldLabels
  where
    mkQualName mk v = QualName (mkImport mPackage moduleComponents)
      { LF.qualObject = mk (T.unpack v)
      }
    mkFieldLabel label = GHC.FieldLabel
      { GHC.flLabel = fsFromText label
      , GHC.flIsOverloaded = True
      , GHC.flSelector = mkQualName mkVarOcc ("$sel:" <> label <> ":" <> value)
      }
    name = mkQualName mkTcOcc value
    pieceNames = mkQualName mkVarOcc <$> pieces
    fieldLabels = mkFieldLabel <$> fields

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

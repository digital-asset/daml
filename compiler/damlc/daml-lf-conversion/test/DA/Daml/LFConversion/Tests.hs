-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import "ghc-lib-parser" OccName (mkDataOcc, mkTcOcc, mkVarOcc)
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
    , roundtripTests "exports" encodeExportInfo decodeExportInfo
        [ ("Foo.Bar (qux)"
          , mkExportInfoVal Nothing ["Foo", "Bar"] "qux")
        , ("\"foo\" Foo.Bar (qux)"
          , mkExportInfoVal (Just "foo") ["Foo", "Bar"] "qux")
        , ("Foo.Bar (Qux())"
          , mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" [] [])
        , ("\"foo\" Foo.Bar (Qux())"
          , mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" [] [])
        , ("Foo.Bar (Qux(getQux))"
          , mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" ["getQux"] [])
        , ("\"foo\" Foo.Bar (Qux(getQux))"
          , mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" ["getQux"] [])
        , ("Foo.Bar (Qux($sel:getQux:Qux))"
          , mkExportInfoTC Nothing ["Foo", "Bar"] "Qux" [] ["getQux"])
        , ("\"foo\" Foo.Bar (Qux($sel:getQux:Qux))"
          , mkExportInfoTC (Just "foo") ["Foo", "Bar"] "Qux" [] ["getQux"])
        ]
    , roundtripTestsBy "type synonyms"
        (\(synName, _isConstraintSyn, _kind, synParams, synType) ->
          (== (synName, synParams, synType)))
        (\(synName, isConstraintSyn, kind, synParams, synType) ->
          encodeTypeSynonym synName isConstraintSyn kind synParams synType)
        decodeTypeSynonym
        [ ( "type Zero = 0"
          , ( LF.TypeSynName ["Zero"]
            , False
            , LF.KNat
            , []
            , LF.TNat (LF.typeLevelNat @Int 0)
            )
          )
        , ( "type IsEnabled = Bool"
          , ( LF.TypeSynName ["IsEnabled"]
            , False
            , LF.KStar
            , []
            , LF.TBuiltin LF.BTBool
            )
          )
        , ( "type Perhaps = Optional"
          , ( LF.TypeSynName ["Perhaps"]
            , False
            , LF.KStar `LF.KArrow` LF.KStar
            , []
            , LF.TBuiltin LF.BTOptional
            )
          )
        , ( "type Possibly a = Optional a"
          , let aVar = LF.TypeVarName "a"
            in
            ( LF.TypeSynName ["Possibly"]
            , False
            , LF.KStar
            , [(aVar, LF.KStar)]
            , LF.TBuiltin LF.BTOptional `LF.TApp` LF.TVar aVar
            )
          )
        , ( "type Num = Number"
          , let numberClass = LF.Qualified LF.PRSelf (LF.ModuleName ["GHC", "Num"]) (LF.TypeSynName ["Number"])
            in
            ( LF.TypeSynName ["Num"]
            , True
            , LF.KStar `LF.KArrow` LF.KStar
            , []
            , LF.TSynApp numberClass []
            )
          )
        , ( "type Display a = Show a"
          , let
              aVar = LF.TypeVarName "a"
              showClass = LF.Qualified LF.PRSelf (LF.ModuleName ["GHC", "Show"]) (LF.TypeSynName ["Show"])
            in
            ( LF.TypeSynName ["Display"]
            , True
            , LF.KStar
            , [(aVar, LF.KStar)]
            , LF.TSynApp showClass [LF.TVar aVar]
            )
          )
        , ( "class C a b; type D = C"
          , let
              cClass = LF.Qualified LF.PRSelf (LF.ModuleName ["Main"]) (LF.TypeSynName ["C"])
            in
            ( LF.TypeSynName ["D"]
            , True
            , LF.KStar `LF.KArrow` LF.KStar `LF.KArrow` LF.KStar
            , []
            , LF.TSynApp cClass []
            )
          )
        , ( "class C a b; type E = C Int"
          , let
              cClass = LF.Qualified LF.PRSelf (LF.ModuleName ["Main"]) (LF.TypeSynName ["C"])
            in
            ( LF.TypeSynName ["E"]
            , True
            , LF.KStar `LF.KArrow` LF.KStar
            , []
            , LF.TSynApp cClass [LF.TBuiltin LF.BTInt64]
            )
          )
        , ( "class C a b; type F a = C a Int"
          , let
              aVar = LF.TypeVarName "a"
              cClass = LF.Qualified LF.PRSelf (LF.ModuleName ["Main"]) (LF.TypeSynName ["C"])
            in
            ( LF.TypeSynName ["F"]
            , True
            , LF.KStar `LF.KArrow` LF.KStar
            , [(aVar, LF.KStar)]
            , LF.TSynApp cClass [LF.TVar aVar, LF.TBuiltin LF.BTInt64]
            )
          )
        ]
    , roundtripTestsBy "fixity declarations"
        -- source text isn't preserved by the encoding,
        -- so we don't consider it for equality.
        (\(name1, GHC.Fixity _sourceText1 precedence1 direction1)
          (name2, GHC.Fixity _sourceText2 precedence2 direction2) ->
            name1 == name2
            && precedence1 == precedence2
            && direction1 == direction2)
        encodeFixityInfo
        decodeFixityInfo
        [ (title, (operatorName, GHC.Fixity mSourceText precedence direction))
        | (operatorName, operatorStr) <-
            [ (mkVarOcc "!!", "!!")
            , (mkVarOcc "??", "??")
            , (mkVarOcc "op", "`op`")
            , (mkVarOcc "Pair", "`Pair`") -- "real" data constructor, see Note [Data Constructors] in GHC
            , (mkDataOcc "Tup", "`Tup`") -- "source" data constructor
            , (mkTcOcc "Either", "`Either`") -- type or class constructor
            , (mkVarOcc ":+", ":+") -- "real" data constructor
            , (mkDataOcc ":*", ":*") -- "source" data constructor
            , (mkTcOcc ":%", ":%") -- type or class constructor
            ]
        , precedence <- [1..9]
        , (direction, directionStr) <-
            [ (GHC.InfixR, "infixr")
            , (GHC.InfixL, "infixl")
            , (GHC.InfixN, "infix")
            ]
        , let str = unwords
                [ directionStr
                , show precedence
                , operatorStr
                ]
        , (title, mSourceText) <-
            [ ( str <> " (without source text)"
              , GHC.NoSourceText)
            , ( str <> " (with source text)"
              , GHC.SourceText str)
            ]
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
roundtripTests groupName = roundtripTestsBy groupName (==)

-- | Like 'roundtripTests', but using the supplied equality predicate
roundtripTestsBy :: String -> (a -> c -> Bool) -> (a -> b) -> (b -> Maybe c) -> [(String, a)] -> TestTree
roundtripTestsBy groupName eq encode decode examples =
    roundtripTestsPartialBy groupName eq (Just . encode) decode [] examples

roundtripTestsPartial :: (Eq a) => String -> (a -> Maybe b) -> (b -> Maybe a) -> [(String, a)] -> [(String, a)] -> TestTree
roundtripTestsPartial groupName = roundtripTestsPartialBy groupName (==)

-- | Like 'roundtripTestsPartial', but using the supplied equality predicate
roundtripTestsPartialBy :: String -> (a -> c -> Bool) -> (a -> Maybe b) -> (b -> Maybe c) -> [(String, a)] -> [(String, a)] -> TestTree
roundtripTestsPartialBy groupName eq encode decode negativeExamples positiveExamples =
    testGroup groupName $
        [ testCase name $
            assertBool "expected value to not have encoding"
                (isNothing (encode value))
        | (name, value) <- negativeExamples
        ] ++
        [ testCase name $
            assertBool "expected value to survive roundtrip"
                (maybe False (eq value) (encode value >>= decode))
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

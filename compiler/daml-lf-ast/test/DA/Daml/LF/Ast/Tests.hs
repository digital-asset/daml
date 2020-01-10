-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Tests
    ( main
    ) where

import Control.Monad (unless)
import Data.Foldable
import Data.List (isInfixOf)
import Data.List.Extra (trim)
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import Text.Read
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Ast.Type
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version
import DA.Daml.LF.Ast.World (initWorld)
import DA.Daml.LF.TypeChecker (checkModule)
import DA.Pretty (renderPretty)

main :: IO ()
main = defaultMain $ testGroup "DA.Daml.LF.Ast"
    [ numericTests
    , substitutionTests
    , typeSynTests
    ]

numericExamples :: [(String, Numeric)]
numericExamples =
    [ ("0.", numeric 0 0)
    , ("42.", numeric 0 42)
    , ("-100.", numeric 0 (-100))
    , ("100.00", numeric 2 10000)
    , ("123.45", numeric 2 12345)
    , ("1.0000", numeric 4 10000)
    , ("1.2345", numeric 4 12345)
    , ("0." ++ replicate 32 '0' ++ "54321", numeric 37 54321)
    , ("-9." ++ replicate 37 '9', numeric 37 (1 - 10^(38::Int)))
    ]

numericTests :: TestTree
numericTests = testGroup "Numeric"
    [ testCase "show" $ do
        for_ numericExamples $ \(str,num) -> do
            assertEqual "show produced wrong string" str (show num)
    , testCase "read" $ do
        for_ numericExamples $ \(str,num) -> do
            assertEqual "read produced wrong numeric or failed" (Just num) (readMaybe str)
    ]


substitutionTests :: TestTree
substitutionTests = testGroup "substitution"
    [ testCase "TForall" $ do
        let subst = Map.fromList [(beta11, vBeta1)]
            ty1 = TForall (beta11, KStar) $ TForall (beta1, KStar) $
                TBuiltin BTArrow `TApp` vBeta11 `TApp` vBeta1
            ty2 = substitute subst ty1
        assertBool "wrong substitution" (alphaEquiv ty1 ty2)
    ]
  where
    beta1 = TypeVarName "beta1"
    beta11 = TypeVarName "beta11"
    vBeta1 = TVar beta1
    vBeta11 = TVar beta11


typeSynTests :: TestTree
typeSynTests =
  testGroup "type synonyms"
  [ testGroup "happy" (map mkHappyTestcase happyExamples)
  , testGroup "sad" (map mkSadTestcase sadExamples)
  , testGroup "bad" (map mkBadTestcase badDefs)
  ] where

  happyExamples :: [(Type,Type)]
  happyExamples =
    [ (TInt64, TInt64)
    , (TParty, TParty)
    , (TSynApp (q myInt) [], TInt64)
    , (TSynApp (q myMyInt) [], TInt64)
    , (TSynApp (q myMyInt) [], TSynApp (q myMyInt) [])
    , (TSynApp (q myPairIP) [], mkPair TInt64 TParty)
    , (TSynApp (q myPairIP) [], mkPair (TSynApp (q myMyInt) []) TParty)
    , (TSynApp (q myIdentity) [TDate], TDate)
    , (TSynApp (q myPairXX) [TDate], mkPair TDate TDate)
    , (TSynApp (q myPairXI) [TDate], mkPair TDate TInt64)
    , (TSynApp (q myPairXI) [TInt64], TSynApp (q myPairXX) [TInt64])
    , (TSynApp (q myIdentity) [TSynApp (q myInt) []], TInt64)
    , (TSynApp (q myIdentity) [TSynApp (q myIdentity) [TParty]], TParty)
    , (TSynApp (q myPairXY) [TDate,TParty], mkPair TDate TParty)
    , (TSynApp (q myHigh) [TBuiltin BTList], TList TInt64)
    , (TSynApp (q myHigh2) [TBuiltin BTArrow,TParty], TParty :-> TParty)
    ]

  sadExamples :: [(Type,Type,String)]
  sadExamples =
    [ (TInt64, TParty, "type mismatch")
    , (TParty, TInt64, "type mismatch")
    , (TParty `TApp` TInt64, TParty, "expected higher kinded type")
    , (TParty, TParty `TApp` TInt64, "expected higher kinded type")
    , (TSynApp (q missing) [], TInt64, "unknown type synonym: M:Missing")
    , (TSynApp (q myInt) [], TParty, "type mismatch")
    , (TSynApp (q myPairIP) [], mkPair TParty TInt64, "type mismatch")
    , (TSynApp (q myIdentity) [], TParty, "wrong arity in type synonym application")
    , (TSynApp (q myIdentity) [TDate], TParty, "type mismatch")
    , (TSynApp (q myPairXI) [TDate], TSynApp (q myPairXX) [TDate], "type mismatch")
    , (TSynApp (q myPairXY) [TDate], TParty, "wrong arity in type synonym application")
    , (TSynApp (q myPairXY) [TDate], TParty, "expected: 2, found: 1")
    , (TSynApp (q myPairXX) [TDate,TParty], TParty, "wrong arity in type synonym application")
    , (TSynApp (q myIdentity) [TBuiltin BTList], TList TInt64,"kind mismatch")
    , (TSynApp (q myHigh) [TBuiltin BTList], TList TParty, "type mismatch")
    , (TSynApp (q myHigh) [TBuiltin BTArrow], TList TInt64,"kind mismatch")
    , (TSynApp (q myHigh) [TInt64], TList TInt64,"kind mismatch")
    , (TSynApp (q myHigh2) [TBuiltin BTList,TParty], TParty :-> TParty, "kind mismatch")
    ]

  goodDefs :: [DefTypeSyn]
  goodDefs =
    [ makeSynDef myInt [] TInt64
    , makeSynDef myMyInt [] (TSynApp (q myInt) [])
    , makeSynDef myPairIP [] (mkPair TInt64 TParty)
    , makeSynDef myIdentity [(x,KStar)] (TVar x)
    , makeSynDef myPairXX [(x,KStar)] (mkPair (TVar x) (TVar x))
    , makeSynDef myPairXI [(x,KStar)] (mkPair (TVar x) TInt64)
    , makeSynDef myPairXY [(x,KStar),(y,KStar)] (mkPair (TVar x) (TVar y))
    , makeSynDef myHigh [(f,KStar `KArrow` KStar)] (TApp (TVar f) TInt64)
    , makeSynDef myHigh2 [(f,KStar `KArrow` KStar `KArrow` KStar),(x,KStar)] (TVar f `TApp` TVar x `TApp` TVar x)
    ]

  badDefs :: [(DefTypeSyn,String)]
  badDefs =
    [ (makeSynDef myBad [] (TVar x), "unknown type variable: x")
    , (makeSynDef myBad [] (TBuiltin BTArrow), "kind mismatch")
    , (makeSynDef myBad [(f,KStar `KArrow` KStar)] (TVar f), "kind mismatch")
    , (makeSynDef myBad [(x,KStar)] (TApp (TVar x) TInt64), "expected higher kinded type")
    , (makeSynDef myBad [(x,KStar),(x,KStar)] (TVar x), "duplicate type parameter: x")
    , (makeSynDef myBad [] (TSynApp (q missing) []), "")
    -- TODO This example should be rejected
    ]

  x,y,f :: TypeVarName
  x = TypeVarName "x"
  y = TypeVarName "y"
  f = TypeVarName "f"

  moduleName :: ModuleName
  moduleName = ModuleName ["M"]

  q :: a -> Qualified a
  q = Qualified PRSelf moduleName

  missing = TypeSynName ["Missing"]
  myInt = TypeSynName ["MyInt"]
  myMyInt = TypeSynName ["MyMyInt"]
  myPairIP = TypeSynName ["MyPairIP"]
  myIdentity = TypeSynName ["MyIdentity"]
  myPairXX = TypeSynName ["MyPairXX"]
  myPairXI = TypeSynName ["MyPairXI"]
  myPairXY = TypeSynName ["MyPairXY"]
  myHigh = TypeSynName ["MyHigh"]
  myHigh2 = TypeSynName ["MyHigh2"]
  myBad = TypeSynName ["MyBad"]

  mkPair ty1 ty2 = TStruct [(FieldName "fst",ty1),(FieldName "snd",ty2)]

  makeSynDef :: TypeSynName -> [(TypeVarName,Kind)] -> Type -> DefTypeSyn
  makeSynDef synName synParams synType = do
    DefTypeSyn { synLocation = Nothing, synName, synParams, synType}

  mkHappyTestcase :: (Type,Type) -> TestTree
  mkHappyTestcase (ty1,ty2) = do
    let name = renderPretty ty1 <> " == " <> renderPretty ty2
    let mod = makeModuleToTestTypeSyns ty1 ty2
    testCase name $ case typeCheck mod of
      Left s -> assertFailure $ "unexpected type error: " <> s
      Right () -> return ()

  mkSadTestcase :: (Type,Type,String) -> TestTree
  mkSadTestcase (ty1,ty2,frag) = do
    let name = renderPretty ty1 <> " =/= " <> renderPretty ty2
    let mod = makeModuleToTestTypeSyns ty1 ty2
    testCase name $ case typeCheck mod of
      Left s -> assertStringContains s (Fragment frag)
      Right () -> assertFailure "expected type error, but got none"

  mkBadTestcase :: (DefTypeSyn,String) -> TestTree
  mkBadTestcase (def,frag) = do
    let name = looseNewlines $ renderPretty def
    let mod = makeModule [def] []
    testCase name $ case typeCheck mod of
      Left s -> assertStringContains s (Fragment frag)
      Right () -> assertFailure "expected type error, but got none"
    where
      looseNewlines = unwords . map trim . lines

  makeModuleToTestTypeSyns :: Type -> Type -> Module
  makeModuleToTestTypeSyns ty1 ty2 = do
    let definition = DefValue
          { dvalLocation = Nothing
          , dvalBinder = (ExprValName "MyFun", ty1 :-> TUnit)
          , dvalNoPartyLiterals = HasNoPartyLiterals True
          , dvalIsTest = IsTest False
          , dvalBody = ETmLam (ExprVarName "ignored", ty2) EUnit
          }
    makeModule goodDefs [definition]

  makeModule :: [DefTypeSyn] -> [DefValue] -> Module
  makeModule synDefs valDefs = do
    Module
      { moduleName
      , moduleSource = Nothing
      , moduleFeatureFlags = FeatureFlags {forbidPartyLiterals = True}
      , moduleSynonyms = NM.fromList synDefs
      , moduleDataTypes = NM.fromList []
      , moduleValues = NM.fromList valDefs
      , moduleTemplates = NM.empty
      }

typeCheck :: Module -> Either String ()
typeCheck mod = do
  let version = V1 (PointStable 7)
  either (Left . renderPretty) Right $ checkModule (initWorld [] version) version mod

assertStringContains :: String -> Fragment -> IO ()
assertStringContains text (Fragment frag) =
  unless (frag `isInfixOf` text) (assertFailure msg)
  where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ text

newtype Fragment = Fragment String

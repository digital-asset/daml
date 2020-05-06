-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Tests
    ( main
    ) where

import Data.Foldable
import Data.List.Extra (trim)
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import Text.Read
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Ast.Type
import DA.Daml.LF.Ast.Alpha
import DA.Daml.LF.Ast.Subst
import DA.Daml.LF.Ast.TypeLevelNat
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version
import DA.Daml.LF.Ast.World (initWorld)
import DA.Daml.LF.TypeChecker (checkModule)
import DA.Pretty (renderPretty)
import DA.Test.Util

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
        let beta1 = TypeVarName "beta1"
            beta11 = TypeVarName "beta11"
            subst = Map.fromList [(beta11, TVar beta1)]
            ty1 = TForall (beta11, KStar) $ TForall (beta1, KStar) $
                TBuiltin BTArrow `TApp` TVar beta11 `TApp` TVar beta1
            ty2 = substitute subst ty1
        assertBool "wrong substitution" (alphaType ty1 ty2)

    , testCase "freeVars/TypeLevelNat" $ do
        let x = TypeVarName "x"
        let y = TypeVarName "y"
        let yRenamed = TypeVarName "yRenamed"
        let typeWithNatAndFreeY = TNat TypeLevelNat10 :-> TVar y
        let subst = Map.fromList [(x, typeWithNatAndFreeY)]
        assertBool "bad substitution" $ alphaType
          (substitute subst (TForall (y,       KStar) $ TVar x              :-> TVar y))
                            (TForall (yRenamed,KStar) $ typeWithNatAndFreeY :-> TVar yRenamed)

    , testCase "ETmLam" $ do
        let x1 = ExprVarName "x1"
            x11 = ExprVarName "x11"
            subst = exprSubst x11 (EVar x1)
            e1 = ETmLam (x11, TInt64) $ ETmLam (x1, TInt64) $
                EBuiltin BEAddInt64 `ETmApp` EVar x11 `ETmApp` EVar x1
            e2 = substExpr subst e1
        assertBool "wrong substitution" (alphaExpr e1 e2)

    ]


typeSynTests :: TestTree
typeSynTests =
  testGroup "type synonyms"
  [ testGroup "happy" (map mkHappyTestcase happyExamples)
  , testGroup "sad" (map mkSadTestcase sadExamples)
  , testGroup "bad" (map mkBadTestcase badDefSets)
  , testGroup "bigger" (map mkBiggerTestcase biggerExamples)
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
    , (TForall (x,KStar) (TVar x), TForall (y,KStar) (TVar y))
    , (TSynApp (q myIdentity) [TForall (x,KStar) (TVar x)], TForall (y,KStar) (TVar y))
    , (TForall (x,KStar) $ TSynApp (q myIdentity) [TVar x], TForall (y,KStar) (TVar y))
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
    , (makeSynDef myBad [] (TSynApp (q missing) []), "unknown type synonym: M:Missing")
    , (makeSynDef myBad [] (TSynApp (q myBad) []), "found type synonym cycle")
    ]

  badDefSets :: [([DefTypeSyn],String)]
  badDefSets = [ ([def],frag) | (def,frag) <- badDefs ] ++
    [
      ([makeSynDef myBad [] (TList (TSynApp (q myBad2) []))
       ,makeSynDef myBad2 [] (TSynApp (q myBad) [])
       ]
      , "found type synonym cycle")
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
  myBad2 = TypeSynName ["MyBad2"]

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
      Left s -> assertInfixOf frag s
      Right () -> assertFailure "expected type error, but got none"

  mkBadTestcase :: ([DefTypeSyn],String) -> TestTree
  mkBadTestcase (defs,frag) = do
    let name = looseNewlines $ renderPretty defs
    let mod = makeModule defs []
    testCase name $ case typeCheck mod of
      Left s -> assertInfixOf frag s
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

  mkBiggerTestcase :: (String,Module) -> TestTree
  mkBiggerTestcase (name,mod) = do
    testCase name $ do
      let _ = putStrLn (renderPretty mod <> "\n")
      case typeCheck mod of
        Left s -> assertFailure $ "unexpected type error: " <> s
        Right () -> return ()

  biggerExamples :: [(String,Module)]
  biggerExamples = [ ("functor/pointed example", functorPointedExample) ]

  functorPointedExample :: Module
  functorPointedExample = makeModule
    [functorDef,pointedDef]
    [identityDef,mapOptionalDef,optionalFunctorDef,fmapDef,optionalPointedDef,pureDef] where

    a = TypeVarName "a"
    b = TypeVarName "b"
    f = TypeVarName "f"

    functor = TypeSynName ["functor"]
    functorDef =
      makeSynDef functor [(f,KStar `KArrow` KStar)] $ TStruct [
      (FieldName "fmap",
       TForall (a,KStar) $
       TForall (b,KStar) $
       (TVar a :-> TVar b) :-> TVar f `TApp` TVar a :-> TVar f `TApp` TVar b
      )]

    pointed = TypeSynName ["pointed"]
    pointedDef =
      makeSynDef pointed [(f,KStar `KArrow` KStar)] $ TStruct [
      (FieldName "super", TSynApp (q functor) [TVar f]),
      (FieldName "pure", TForall (a,KStar) $ TVar a :-> TVar f `TApp` TVar a)
      ]

    dict = ExprVarName "dict"
    opt = ExprVarName "opt"
    x = ExprVarName "x"
    func = ExprVarName "func"

    identityDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "identity", TForall (a,KStar) $ TVar a :-> TVar a)
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = ETyLam (a,KStar) $ ETmLam (opt,TVar a) (EVar opt)
      }

    mapOptional = ExprValName "mapOptional"
    mapOptionalDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mapOptional, mapOptionalType)
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = mapOptionalExp
      }

    mapOptionalType =
      TForall (a,KStar) $
      TForall (b,KStar) $
      (TVar a :-> TVar b) :->
      TOptional (TVar a) :->
      TOptional (TVar b)

    mapOptionalExp =
      ETyLam (a,KStar) $
      ETyLam (b,KStar) $
      ETmLam (func,TVar a :-> TVar b) $
      ETmLam (opt,TOptional (TVar a)) $
      ECase (EVar opt)
      [ CaseAlternative CPNone (ENone (TVar b))
      , CaseAlternative (CPSome x) (ESome (TVar b) (EVar func `ETmApp` EVar x))
      ]

    optionalFunctor = ExprValName "optionalFunctor"
    optionalFunctorDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (optionalFunctor, TSynApp (q functor) [TBuiltin BTOptional])
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EStructCon [(FieldName "fmap", EVal (q mapOptional))]
      }

    optionalPointedDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "optionalPointed", TSynApp (q pointed) [TBuiltin BTOptional])
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EStructCon [(FieldName "super", EVal (q optionalFunctor))
                              ,(FieldName "pure",
                                ETyLam (a,KStar) $ ETmLam (x,TVar a) $ ESome (TVar a) (EVar x))]
      }

    fmapDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "fmap", fmapType)
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody =
        ETyLam (f,KStar `KArrow` KStar) $
        ETmLam (dict,TSynApp (q functor) [TVar f]) $
        EStructProj (FieldName "fmap") $
        EVar dict
      }

    fmapType =
      TForall (f,KStar `KArrow` KStar) $
      TSynApp (q functor) [TVar f] :-> (
      TForall (a,KStar) $
      TForall (b,KStar) $
      (TVar a :-> TVar b) :->
      TVar f `TApp` TVar a :->
      TVar f `TApp` TVar b )

    pureDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "pure", pureType)
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody =
        ETyLam (f,KStar `KArrow` KStar) $
        ETmLam (dict,TSynApp (q pointed) [TVar f]) $
        EStructProj (FieldName "pure") $
        EVar dict
      }

    pureType =
      TForall (f,KStar `KArrow` KStar) $
      TSynApp (q pointed) [TVar f] :-> (
      TForall (a,KStar) $
      TVar a :->
      TVar f `TApp` TVar a )


typeCheck :: Module -> Either String ()
typeCheck mod = do
  let version = V1 (PointStable 7)
  let diags = checkModule (initWorld [] version) version mod
  if null diags then Right () else Left (show diags)

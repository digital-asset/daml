-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    , alphaTests
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

alphaTests :: TestTree
alphaTests = testGroup "alpha equivalence"
    [ testCase "alphaExpr" $ do
        let assertAlpha a b =
                assertBool (show a <> " should be alpha equivalent to " <> show b) $
                    alphaExpr a b
            assertNotAlpha a b =
                assertBool (show a <> " should not be alpha equivalent to " <> show b) $
                    not (alphaExpr a b)



        assertAlpha
            (EVar (ExprVarName "x"))
            (EVar (ExprVarName "x"))
        assertNotAlpha
            (EVar (ExprVarName "x"))
            (EVar (ExprVarName "y"))
        assertAlpha
            (ETmLam (ExprVarName "x", TInt64) (EVar (ExprVarName "x")))
            (ETmLam (ExprVarName "y", TInt64) (EVar (ExprVarName "y")))
        assertNotAlpha
            (ETmLam (ExprVarName "x", TInt64) (EVar (ExprVarName "x")))
            (ETmLam (ExprVarName "y", TText) (EVar (ExprVarName "y")))
        assertNotAlpha
            (ETmLam (ExprVarName "x", TVar (TypeVarName "a")) (EVar (ExprVarName "x")))
            (ETmLam (ExprVarName "y", TVar (TypeVarName "b")) (EVar (ExprVarName "y")))
        assertAlpha
            (ETyLam (TypeVarName "a", KStar) (ETmLam (ExprVarName "x", TVar (TypeVarName "a")) (EVar (ExprVarName "x"))))
            (ETyLam (TypeVarName "b", KStar) (ETmLam (ExprVarName "y", TVar (TypeVarName "b")) (EVar (ExprVarName "y"))))
        assertAlpha
            (ETmLam (ExprVarName "x", TInt64) (ETmLam (ExprVarName "y", TInt64) (EVar (ExprVarName "x"))))
            (ETmLam (ExprVarName "y", TInt64) (ETmLam (ExprVarName "x", TInt64) (EVar (ExprVarName "y"))))
        assertNotAlpha
            (ETmLam (ExprVarName "x", TInt64) (ETmLam (ExprVarName "y", TInt64) (EVar (ExprVarName "x"))))
            (ETmLam (ExprVarName "y", TInt64) (ETmLam (ExprVarName "x", TInt64) (EVar (ExprVarName "x"))))
        assertAlpha
            (ELet (Binding (ExprVarName "x", TInt64) (ENone TInt64)) (EVar (ExprVarName "x")))
            (ELet (Binding (ExprVarName "y", TInt64) (ENone TInt64)) (EVar (ExprVarName "y")))
        assertNotAlpha -- NOTE: "let" is not recursive in Daml-LF
            (ELet (Binding (ExprVarName "x", TInt64) (EVar (ExprVarName "x"))) (EVar (ExprVarName "x")))
            (ELet (Binding (ExprVarName "y", TInt64) (EVar (ExprVarName "y"))) (EVar (ExprVarName "y")))
        assertAlpha
            (ELet (Binding (ExprVarName "x", TInt64) (EVar (ExprVarName "x"))) (EVar (ExprVarName "x")))
            (ELet (Binding (ExprVarName "y", TInt64) (EVar (ExprVarName "x"))) (EVar (ExprVarName "y")))
        assertNotAlpha
            (ECase (ENone TInt64) [CaseAlternative CPNone (ENone TInt64)])
            (ECase (ENone TInt64) [CaseAlternative (CPSome (ExprVarName "x")) (ENone TInt64)])
        assertAlpha
            (ECase (ENone TInt64) [CaseAlternative (CPSome (ExprVarName "x")) (EVar (ExprVarName "x"))])
            (ECase (ENone TInt64) [CaseAlternative (CPSome (ExprVarName "y")) (EVar (ExprVarName "y"))])
        assertNotAlpha
            (ECase (ENone TInt64) [CaseAlternative (CPSome (ExprVarName "x")) (EVar (ExprVarName "x"))])
            (ECase (ENone TInt64) [CaseAlternative (CPSome (ExprVarName "y")) (EVar (ExprVarName "x"))])
        assertAlpha
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "a") (ExprVarName "b")) (EVar (ExprVarName "a"))])
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "x") (ExprVarName "y")) (EVar (ExprVarName "x"))])
        assertNotAlpha
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "a") (ExprVarName "b")) (EVar (ExprVarName "a"))])
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "x") (ExprVarName "y")) (EVar (ExprVarName "y"))])
        assertAlpha
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "a") (ExprVarName "b")) (EVar (ExprVarName "b"))])
            (ECase (ENil TInt64) [CaseAlternative (CPCons (ExprVarName "x") (ExprVarName "y")) (EVar (ExprVarName "y"))])
        -- Don't need tests for CPCons pattern variable names being
        -- the same because that is not allowed, caught by typechecker.
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

    , testCase "applySubstInExpr" $ do
        let x = ExprVarName "x"
            x1 = ExprVarName "x1"
            y = ExprVarName "y"
            z = ExprVarName "z"
            a = TypeVarName "a"
            a1 = TypeVarName "a1"
            b = TypeVarName "b"
            c = TypeVarName "c"

            assertExprSubst x e a b =
                let subst = exprSubst x e
                    test = alphaExpr (applySubstInExpr subst a) b
                    msg = unlines
                        ["substitution test failed:"
                        , "\tsubst = exprSubst (" <> show x <> ") (" <> show e <> ")"
                        , "\toriginal = " <> show a
                        , "\texpected = " <> show b
                        , "\tgot = " <> show (applySubstInExpr subst a)
                        ]
                in assertBool msg test

            assertTypeSubst x t a b =
                let subst = typeSubst x t
                    test = alphaExpr (applySubstInExpr subst a) b
                    msg = unlines
                        ["substitution test failed:"
                        , "\tsubst = typeSubst (" <> show x <> ") (" <> show t <> ")"
                        , "\toriginal = " <> show a
                        , "\texpected = " <> show b
                        , "\tgot = " <> show (applySubstInExpr subst a)
                        ]
                in assertBool msg test

        -- no binder tests
        assertExprSubst x (EBuiltin (BEInt64 42))
            (EVar y)
            (EVar y)
        assertExprSubst x (EBuiltin (BEInt64 42))
            (EVar x)
            (EBuiltin (BEInt64 42))
        assertExprSubst x (EBuiltin (BEInt64 42))
            (EBuiltin BEAddInt64 `ETmApp` EVar x `ETmApp` EVar y)
            (EBuiltin BEAddInt64 `ETmApp` EBuiltin (BEInt64 42) `ETmApp` EVar y)
        assertTypeSubst a TInt64
            (ENone (TVar b))
            (ENone (TVar b))
        assertTypeSubst a TInt64
            (ENone (TVar a))
            (ENone TInt64)
        assertTypeSubst a TInt64
            (ENone (TVar a :-> TVar b))
            (ENone (TInt64 :-> TVar b))

        -- bound variable matches substitution variable
        assertExprSubst x (EBuiltin (BEInt64 42))
            (ETmLam (x, TInt64) (EVar x))
            (ETmLam (x, TInt64) (EVar x))
        assertTypeSubst a TInt64
            (ETyLam (a, KStar) (ENone (TVar a)))
            (ETyLam (a, KStar) (ENone (TVar a)))
        assertTypeSubst a TInt64
            (ENone (TForall (a, KStar) (TVar a)))
            (ENone (TForall (a, KStar) (TVar a)))

        -- bound variable does not match substitution variable nor appears free in substitution body
        assertExprSubst x (EBuiltin (BEInt64 42))
            (ETmLam (y, TInt64) (EVar x))
            (ETmLam (y, TInt64) (EBuiltin (BEInt64 42)))
        assertExprSubst x (EBuiltin (BEInt64 42))
            (ETmLam (y, TInt64) (EVar y))
            (ETmLam (y, TInt64) (EVar y))
        assertExprSubst x (EBuiltin (BEInt64 42))
            (ETmLam (y, TInt64) (EVar z))
            (ETmLam (y, TInt64) (EVar z))
        assertTypeSubst a TInt64
            (ETyLam (b, KStar) (ENone (TVar a)))
            (ETyLam (b, KStar) (ENone TInt64))
        assertTypeSubst a TInt64
            (ETyLam (b, KStar) (ENone (TVar b)))
            (ETyLam (b, KStar) (ENone (TVar b)))
        assertTypeSubst a TInt64
            (ETyLam (b, KStar) (ENone (TVar c)))
            (ETyLam (b, KStar) (ENone (TVar c)))
        assertTypeSubst a TInt64
            (ENone (TForall (b, KStar) (TVar a)))
            (ENone (TForall (b, KStar) TInt64))
        assertTypeSubst a TInt64
            (ENone (TForall (b, KStar) (TVar b)))
            (ENone (TForall (b, KStar) (TVar b)))
        assertTypeSubst a TInt64
            (ENone (TForall (b, KStar) (TVar c)))
            (ENone (TForall (b, KStar) (TVar c)))

        -- mixture of both cases above
        assertExprSubst x (EBuiltin (BEInt64 42))
            (ETmLam (y, TInt64) (ETmLam (x, TInt64) (EVar x) `ETmApp` EVar x))
            (ETmLam (y, TInt64) (ETmLam (x, TInt64) (EVar x) `ETmApp` EBuiltin (BEInt64 42)))
        assertTypeSubst a TInt64
            (ETyLam (b, KStar) (ETyLam (a, KStar) (ENone (TVar a)) `ETmApp` ENone (TVar a)))
            (ETyLam (b, KStar) (ETyLam (a, KStar) (ENone (TVar a)) `ETmApp` ENone TInt64))
        assertTypeSubst a TInt64
            (ETyLam (b, KStar) (ENone (TForall (a, KStar) (TVar a) :-> TVar a)))
            (ETyLam (b, KStar) (ENone (TForall (a, KStar) (TVar a) :-> TInt64)))

        -- bound variable appears in substitution body
        assertExprSubst x (EVar y)
            (ETmLam (y, TInt64) (EBuiltin (BEInt64 42)))
            (ETmLam (y, TInt64) (EBuiltin (BEInt64 42)))
        assertExprSubst x (EVar y)
            (ETmLam (y, TInt64) (EVar x))
            (ETmLam (z, TInt64) (EVar y))
        assertTypeSubst a (TVar b)
            (ETyLam (b, KStar) (ENone TInt64))
            (ETyLam (b, KStar) (ENone TInt64))
        assertTypeSubst a (TVar b)
            (ETyLam (b, KStar) (ENone (TVar a)))
            (ETyLam (c, KStar) (ENone (TVar b)))
        assertTypeSubst a (TVar b)
            (ENone (TForall (b, KStar) TInt64))
            (ENone (TForall (b, KStar) TInt64))
        assertTypeSubst a (TVar b)
            (ENone (TForall (b, KStar) (TVar a)))
            (ENone (TForall (c, KStar) (TVar b)))

        -- interaction between fresh variables & binders (a port of the TForall regression test)
        assertExprSubst x1 (EVar x)
            (ETmLam (x1, TInt64) (ETmLam (x, TInt64) (EVar x)))
            (ETmLam (x1, TInt64) (ETmLam (x, TInt64) (EVar x)))
        assertExprSubst x1 (EVar x)
            (ETmLam (x1, TInt64) (ETmLam (x, TInt64) (EVar x1)))
            (ETmLam (x1, TInt64) (ETmLam (x, TInt64) (EVar x1)))
        assertTypeSubst a1 (TVar a)
            (ETyLam (a1, KStar) (ETyLam (a, KStar) (ENone (TVar a))))
            (ETyLam (a1, KStar) (ETyLam (a, KStar) (ENone (TVar a))))
        assertTypeSubst a1 (TVar a)
            (ETyLam (a1, KStar) (ETyLam (a, KStar) (ENone (TVar a1))))
            (ETyLam (a1, KStar) (ETyLam (a, KStar) (ENone (TVar a1))))
        assertTypeSubst a1 (TVar a)
            (ENone (TForall (a1, KStar) (TForall (a, KStar) (TVar a))))
            (ENone (TForall (a1, KStar) (TForall (a, KStar) (TVar a))))
        assertTypeSubst a1 (TVar a)
            (ENone (TForall (a1, KStar) (TForall (a, KStar) (TVar a1))))
            (ENone (TForall (a1, KStar) (TForall (a, KStar) (TVar a1))))

    ]


typeSynTests :: TestTree
typeSynTests =
  testGroup "type synonyms" $
  [ testGroup (renderVersion version)
    [ testGroup "happy" (map (mkHappyTestcase version) happyExamples)
    , testGroup "sad" (map (mkSadTestcase version) sadExamples)
    , testGroup "bad" (map (mkBadTestcase version) badDefSets)
    , testGroup "bigger" (map (mkBiggerTestcase version) biggerExamples)
    ]
    | version <- [version1_dev, version2_dev]
  ]
  where

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

  mkHappyTestcase :: Version -> (Type,Type) -> TestTree
  mkHappyTestcase version (ty1,ty2) = do
    let name = renderPretty ty1 <> " == " <> renderPretty ty2
    let mod = makeModuleToTestTypeSyns ty1 ty2
    testCase name $ case typeCheck version mod of
      Left s -> assertFailure $ "unexpected type error: " <> s
      Right () -> return ()

  mkSadTestcase :: Version -> (Type,Type,String) -> TestTree
  mkSadTestcase version (ty1,ty2,frag) = do
    let name = renderPretty ty1 <> " =/= " <> renderPretty ty2
    let mod = makeModuleToTestTypeSyns ty1 ty2
    testCase name $ case typeCheck version mod of
      Left s -> assertInfixOf frag s
      Right () -> assertFailure "expected type error, but got none"

  mkBadTestcase :: Version -> ([DefTypeSyn],String) -> TestTree
  mkBadTestcase version (defs,frag) = do
    let name = looseNewlines $ renderPretty defs
    let mod = makeModule defs []
    testCase name $ case typeCheck version mod of
      Left s -> assertInfixOf frag s
      Right () -> assertFailure "expected type error, but got none"
    where
      looseNewlines = unwords . map trim . lines

  makeModuleToTestTypeSyns :: Type -> Type -> Module
  makeModuleToTestTypeSyns ty1 ty2 = do
    let definition = DefValue
          { dvalLocation = Nothing
          , dvalBinder = (ExprValName "MyFun", ty1 :-> TUnit)
          , dvalIsTest = IsTest False
          , dvalBody = ETmLam (ExprVarName "ignored", ty2) EUnit
          }
    makeModule goodDefs [definition]

  makeModule :: [DefTypeSyn] -> [DefValue] -> Module
  makeModule synDefs valDefs = do
    Module
      { moduleName
      , moduleSource = Nothing
      , moduleFeatureFlags = FeatureFlags
      , moduleSynonyms = NM.fromList synDefs
      , moduleDataTypes = NM.fromList []
      , moduleValues = NM.fromList valDefs
      , moduleTemplates = NM.empty
      , moduleExceptions = NM.empty
      , moduleInterfaces = NM.empty
      }

  mkBiggerTestcase :: Version -> (String,Module) -> TestTree
  mkBiggerTestcase version (name,mod) = do
    testCase name $ do
      let _ = putStrLn (renderPretty mod <> "\n")
      case typeCheck version mod of
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
      , dvalIsTest = IsTest False
      , dvalBody = ETyLam (a,KStar) $ ETmLam (opt,TVar a) (EVar opt)
      }

    mapOptional = ExprValName "mapOptional"
    mapOptionalDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mapOptional, mapOptionalType)
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
      , dvalIsTest = IsTest False
      , dvalBody = EStructCon [(FieldName "fmap", EVal (q mapOptional))]
      }

    optionalPointedDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "optionalPointed", TSynApp (q pointed) [TBuiltin BTOptional])
      , dvalIsTest = IsTest False
      , dvalBody = EStructCon [(FieldName "super", EVal (q optionalFunctor))
                              ,(FieldName "pure",
                                ETyLam (a,KStar) $ ETmLam (x,TVar a) $ ESome (TVar a) (EVar x))]
      }

    fmapDef = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (ExprValName "fmap", fmapType)
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


typeCheck :: Version -> Module -> Either String ()
typeCheck version mod = do
  let diags = checkModule (initWorld [] version) version mod
  if null diags then Right () else Left (show diags)

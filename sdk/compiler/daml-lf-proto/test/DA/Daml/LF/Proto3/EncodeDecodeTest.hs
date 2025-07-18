-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeDecodeTest (
        module DA.Daml.LF.Proto3.EncodeDecodeTest
      , pPrint
) where


import qualified Data.NameMap                             as NM
import           Data.Text


import           DA.Daml.LF.Proto3.Encode
import           DA.Daml.LF.Proto3.Decode
import           DA.Daml.Compiler.ExtractDar

import qualified DA.Daml.LF.Proto3.Archive as Archive

import qualified Data.ByteString.Lazy as BSL
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Text.Pretty.Simple                       as PP

import           Test.Tasty.HUnit
import           Test.Tasty

-- import           DA.Bazel.Runfiles
import           System.FilePath

entry :: IO ()
entry = defaultMain $ testGroup "All tests" [ rttTests ]



------------------------------------------------------------------------
-- .daml files tests
------------------------------------------------------------------------
bla :: IO ()
bla = do
  -- scriptDar <- locateRunfiles $ mainWorkspace </> darPath
  let scriptDar = "/home/roger.bosman/Downloads/thedar.dar"
  PP.pPrint scriptDar

  dar <- extractDar scriptDar

  let mainDalf = edMain dar

  let bs = BSL.toStrict $ ZipArchive.fromEntry mainDalf
  let (_pkgId, pkg) = case Archive.decodeArchive Archive.DecodeAsMain bs of
        Right x -> x
        Left _ -> error "d'oh"

  defaultMain $ testCase "bla" $ roundTripAssert pkg

  where
    _darPath = "daml-script" </> "test" </> "script-test-v2.dev.dar"

------------------------------------------------------------------------
-- Round-trip
------------------------------------------------------------------------
roundTripPackage :: Package -> Either Error Package
roundTripPackage p = (decode . encodePackage) p
  where
    decode :: P.Package -> Either Error Package
    decode = decodePackage
      (packageLfVersion p) --from passed package
      SelfPackageId


roundTripAssert :: Package -> Assertion
roundTripAssert p =
  either
    (\err -> assertFailure $ "Unexpected error: " ++ show err)
    (\val -> p @=? val)
    (roundTripPackage p)

rttTests :: TestTree
rttTests = testGroup "Round-trip tests"
    [ rttEmpty
    , rttTyLam
    , rttTyLamAndLet
    , rttDeepLetWithLoc
    ]

rttEmpty :: TestTree
rttEmpty = testCase "empty package" $ roundTripAssert $ mkOneModulePackage mkEmptyModule

rttTyLam :: TestTree
rttTyLam = testCase "tylam package" $ roundTripAssert $ mkOneModulePackage tyLamModule

rttTyLamAndLet :: TestTree
rttTyLamAndLet = testCase "tylam and let package" $ roundTripAssert $ mkOneModulePackage tyLamAndLetModule

rttDeepLetWithLoc :: TestTree
rttDeepLetWithLoc = testCase "deep let with location package" $ roundTripAssert $ mkOneModulePackage mkDeepLetWithLocModule

------------------------------------------------------------------------
-- Examples
------------------------------------------------------------------------

tyLamModule :: Module
tyLamModule = mkEmptyModule{moduleValues = NM.singleton mkDefTyLamDef}

tyLamAndLetModule :: Module
tyLamAndLetModule = mkEmptyModule{moduleValues = NM.fromList [mkDefTyLamDef, mkLet]}

mkDeepLetWithLocModule :: Module
mkDeepLetWithLocModule = mkEmptyModule{moduleValues = NM.singleton mkDeepLetWithLoc}

testLoc :: SourceLoc
testLoc = SourceLoc{..}
    where
    slocModuleRef = Nothing
    slocStartLine = 42
    slocEndLine   = 36
    slocStartCol  = 24
    slocEndCol    = 12

a :: TypeVarName
a = TypeVarName "a"

x :: ExprVarName
x = ExprVarName "x"

f, lt :: ExprValName
f = ExprValName "f"
lt = ExprValName "lt"

elam :: Expr
elam = ETmLam (x, TVar a) (EVar x)

-- Λa . λ (x : a) . x
tyLam :: Expr
tyLam = ETyLam (a, typToTyp) elam

typToTyp :: Kind
typToTyp = KArrow KStar KStar

{-
f :: forall (a : * -> *). a -> a
f = Λa . λ (x : a) . x
-}
mkDefTyLamDef :: DefValue
mkDefTyLamDef = DefValue (Just testLoc) (f, tyLamTyp) tyLam

elet :: Text -> Type -> Expr -> Expr -> Expr
elet x t e1 e2 = ELet (Binding (ExprVarName x, t) e1) e2

mkLet :: DefValue
mkLet = DefValue (Just testLoc) (lt, TUnit) (elet "id" tyLamTyp tyLam (EVal $ eQual lt))

test :: IO ()
test = PP.pPrint $ encodePackage $ mkOneModulePackage tyLamAndLetModule

orig :: IO ()
orig = PP.pPrint $ mkOneModulePackage mkDeepLetWithLocModule

enc :: IO ()
enc = PP.pPrint $ encodePackage $ mkOneModulePackage mkDeepLetWithLocModule

rtt :: IO ()
rtt = case roundTripPackage $ mkOneModulePackage mkDeepLetWithLocModule of
  Right p -> PP.pPrint p
  _ -> error "foo"



mkDeepLetWithLoc :: DefValue
mkDeepLetWithLoc = DefValue (Just testLoc) (lt, TUnit) (letOfDepthWithLoc 1)

letOfDepthWithLoc :: Int -> Expr
letOfDepthWithLoc 0 = EUnit
letOfDepthWithLoc i = elet (letOfDepthWithLoc $ i -1)
  where
    elet :: Expr -> Expr
    elet e = ELet unitBinding (ELocation testLoc e)

    unitBinding :: Binding
    unitBinding = Binding (ExprVarName "x", TUnit) EUnit

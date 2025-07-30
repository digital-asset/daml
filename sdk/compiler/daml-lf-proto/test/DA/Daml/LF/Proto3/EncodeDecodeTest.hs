-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

 {- HLINT ignore "locateRunfiles/package_app" -}

module DA.Daml.LF.Proto3.EncodeDecodeTest (
        module DA.Daml.LF.Proto3.EncodeDecodeTest
) where


import qualified Data.NameMap                             as NM
import           Data.Text                                hiding (map)


import           DA.Daml.LF.Proto3.Encode
import           DA.Daml.LF.Proto3.Decode
import           DA.Daml.Compiler.ExtractDar

import qualified DA.Daml.LF.Proto3.Archive as Archive

import qualified Data.ByteString.Lazy as BSL
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

import           DA.Bazel.Runfiles
import           System.FilePath

entry :: IO ()
entry = defaultMain $ testGroup "Round-trip tests"
    [ rttEmpty
    , rttTyLam
    , rttTyLamAndLet
    , rttDeepLetWithLoc
    , darTests
    ]


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

rttEmpty :: TestTree
rttEmpty = testCase "empty package" $ roundTripAssert $ mkOneModulePackage mkEmptyModule

rttTyLam :: TestTree
rttTyLam = testCase "tylam package" $ roundTripAssert $ mkOneModulePackage tyLamModule

rttTyLamAndLet :: TestTree
rttTyLamAndLet = testCase "tylam and let package" $ roundTripAssert $ mkOneModulePackage tyLamAndLetModule

rttDeepLetWithLoc :: TestTree
rttDeepLetWithLoc = testCase "deep let with location package" $ roundTripAssert $ mkOneModulePackage mkDeepLetWithLocModule

------------------------------------------------------------------------
-- .dar tests
------------------------------------------------------------------------
darTests :: TestTree
darTests = testGroup ".dar tests" $ rttDar <$>
    [ "script-test-v2.dev.dar"
    ]


rttDar :: String -> TestTree
rttDar darname = testCase darname $ do
  scriptDar <- locateRunfiles $ mainWorkspace </> darPath

  dar <- extractDar scriptDar
  let mainDalf = edMain dar
  let bs = BSL.toStrict $ ZipArchive.fromEntry mainDalf
  case Archive.decodeArchive Archive.DecodeAsMain bs of
    Right (_, p) -> roundTripAssert p
    _            -> error "decodeArchive failiure"
  where
    darPath = "daml-script" </> "test" </> darname

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
mkLet = DefValue (Just testLoc) (lt, TUnit) (elet "id" tyLamTyp tyLam (EVal $ eQualTest lt))

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

-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.DecodeTest (
        module DA.Daml.LF.Proto3.DecodeTest
) where


import           Data.List                                (isInfixOf)
import qualified Data.Vector                              as V
import           Data.Vector                              (empty)

import           DA.Daml.LF.Proto3.DecodeV2

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Proto3.Error
import           DA.Daml.LF.Proto3.Util
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests" [ decTests ]

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------
testVersion :: Version
testVersion = Version V2 PointDev

------------------------------------------------------------------------
-- DecodeTests
------------------------------------------------------------------------
decTests :: TestTree
decTests = testGroup "decoding tests"
  [ decPureTests
  , decExternalCallTests
  , decInterningTests
  ]

emptyDecodeEnv :: DecodeEnv
emptyDecodeEnv = DecodeEnv empty empty empty empty empty SelfPackageId testVersion (Left $ noPkgImportsReasonTesting "DA.Daml.LF.Proto3.DecodeTest:emptyDecodeEnv ")

decodeKindAssert :: P.Kind -> Kind -> Assertion
decodeKindAssert pk k =
  either
    (\err -> assertFailure $ "Unexpected error: " ++ show err)
    (\k' -> k @=? k')
    (runDecode emptyDecodeEnv (decodeKind pk))

decodeKindTest :: String -> P.Kind -> Kind -> TestTree
decodeKindTest str pk k = testCase str $ decodeKindAssert pk k

decodeExprWithVersion :: Version -> P.Expr -> Either Error Expr
decodeExprWithVersion lfVersion pe =
  runDecode emptyDecodeEnv{version = lfVersion} (decodeExpr pe)

decPureTests :: TestTree
decPureTests = testGroup "decoding tests (non-interning)" $ map (uncurry3 decodeKindTest)
  [ ("Kind star", pkstar, KStar)
  , ("Kind Nat", pknat, KNat)
  , ("star to star", pkarr pkstar pkstar, KArrow KStar KStar)
  , ("(star to nat) to star", pkarr (pkarr pkstar pknat) pkstar, KArrow (KArrow KStar KNat) KStar)
  ]
  where
    uncurry3 :: (a -> b -> c -> d) -> ((a, b, c) -> d)
    uncurry3 f (a, b, c) = f a b c

decExternalCallTests :: TestTree
decExternalCallTests = testGroup "external call feature gate"
  [ testCase "EXTERNAL_CALL decodes inside feature version range" $
      Right (EBuiltinFun BEExternalCall) @=? decodeExprWithVersion testVersion externalCallExpr
  , testCase "EXTERNAL_CALL rejects outside feature version range" $
      assertExternalCallUnsupported $ decodeExprWithVersion version2_3 externalCallExpr
  ]
  where
    externalCallExpr = peBuiltin P.BuiltinFunctionEXTERNAL_CALL

    assertExternalCallUnsupported :: Either Error Expr -> Assertion
    assertExternalCallUnsupported result =
      case result of
        Left (ParseError msg)
          | "does not support External Call" `isInfixOf` msg -> pure ()
        other -> assertFailure $ "expected unsupported External Call decode error, but got: " <> show other

decInterningTests :: TestTree
decInterningTests = testGroup "decoding tests (interning)"
  [ decInterningStarToStar
  ]


decInterningStarToStar :: TestTree
decInterningStarToStar =
  let kinds = V.singleton (KArrow KStar KStar)
      env = emptyDecodeEnv{internedKinds = kinds}
  in  testCase "star to star" $ either
        (\err -> assertFailure $ "Unexpected error: " ++ show err)
        (\k -> k @=? KArrow KStar KStar)
        (runDecode env (decodeKind (pkinterned 0)))

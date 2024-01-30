-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Simplifier.Tests
    ( main
    ) where

import Test.Tasty
import Test.Tasty.HUnit
import qualified Data.NameMap as NM
import qualified Data.Text as T

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version (version2_dev, Version, renderVersion)
import DA.Daml.LF.Ast.World (initWorld)
import DA.Daml.LF.Simplifier (simplifyModule)


main :: IO ()
main = defaultMain $ testGroup "DA.Daml.LF.Simplifier"
    [ constantLiftingTests version2_dev ]

-- The Simplifier calls the typechecker whose behavior is affected by feature
-- flags. The simplifier may thus behave differently based on the version of LF
-- and thus we may need to test different LF versions as they diverge over time.
constantLiftingTests :: Version -> TestTree
constantLiftingTests version = testGroup ("Constant Lifting " <> renderVersion version)
    [ mkTestCase "empty module" [] []
    , mkTestCase "closed value"
        [ dval "foo" TInt64 (EBuiltin (BEInt64 10)) ]
        [ dval "foo" TInt64 (EBuiltin (BEInt64 10)) ]
    , mkTestCase "nested int"
        [ dval "foo" (TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64) (EBuiltin (BEInt64 10))) ]
        [ dval "foo" (TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64) (EBuiltin (BEInt64 10))) ]
    , mkTestCase "nested arithmetic"
        [ dval "foo" (TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64)
                (EBuiltin BEAddInt64
                    `ETmApp` EBuiltin (BEInt64 10)
                    `ETmApp` EBuiltin (BEInt64 10)))
        ]
        [ dval "$$sc_foo_1" TInt64
            (EBuiltin BEAddInt64
                `ETmApp` EBuiltin (BEInt64 10)
                `ETmApp` EBuiltin (BEInt64 10))
        , dval "foo" (TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64) (exprVal "$$sc_foo_1"))
        ]
    , mkTestCase "\\xy.y" -- test that we aren't breaking up λxy.y into two lambdas.
        [ dval "foo" (TInt64 :-> TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64)
                (ETmLam (ExprVarName "y", TInt64)
                    (EVar (ExprVarName "y"))))
        ]
        [ dval "foo" (TInt64 :-> TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64)
                (ETmLam (ExprVarName "y", TInt64)
                    (EVar (ExprVarName "y"))))
        ]
    , mkTestCase "\\z.(\\xy.y)z" -- test that we're lifting closed lambda subexpressions
        [ dval "foo" (TInt64 :-> TInt64 :-> TInt64)
            (ETmLam (ExprVarName "z", TInt64)
                (ETmApp
                    (ETmLam (ExprVarName "x", TInt64)
                        (ETmLam (ExprVarName "y", TInt64)
                            (EVar (ExprVarName "y"))))
                    (EVar (ExprVarName "z"))))
        ]
        [ dval "$$sc_foo_1" (TInt64 :-> TInt64 :-> TInt64)
            (ETmLam (ExprVarName "x", TInt64)
                (ETmLam (ExprVarName "y", TInt64)
                    (EVar (ExprVarName "y"))))
        , dval "foo" (TInt64 :-> TInt64 :-> TInt64)
            (ETmLam (ExprVarName "z", TInt64)
                (ETmApp
                    (exprVal "$$sc_foo_1")
                    (EVar (ExprVarName "z"))))
            -- NOTE: this is a candidate for eta reduction, may be optimized in the future
        ]
    ]
  where
    mkTestCase :: String -> [DefValue] -> [DefValue] -> TestTree
    mkTestCase msg vs1 vs2 =
        testCase msg $ assertEqual "should be equal"
            vs2 (simplifyValues vs1)

    dval :: T.Text -> Type -> Expr -> DefValue
    dval name ty body = DefValue
        { dvalLocation = Nothing
        , dvalBinder = (ExprValName name, ty)
        , dvalIsTest = IsTest False
        , dvalBody = body
        }

    simplifyValues vs = NM.toList . moduleValues $
        simplifyModule world version Module
            { moduleName = ModuleName ["M"]
            , moduleSource = Nothing
            , moduleFeatureFlags = daml12FeatureFlags
            , moduleSynonyms = NM.empty
            , moduleDataTypes = NM.empty
            , moduleTemplates = NM.empty
            , moduleValues = NM.fromList vs
            , moduleExceptions = NM.empty
            , moduleInterfaces = NM.empty
            }
    world = initWorld [] version

    qualify :: t -> Qualified t
    qualify x = Qualified
        { qualPackage = PRSelf
        , qualModule = ModuleName ["M"]
        , qualObject = x
        }

    exprVal :: T.Text -> Expr
    exprVal = EVal . qualify . ExprValName

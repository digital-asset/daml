module DA.Daml.LF.Simplifier.Tests where

import Test.Tasty
import Test.Tasty.HUnit
import qualified Data.NameMap as NM
import qualified Data.Text as T

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Version (versionDev)
import DA.Daml.LF.Ast.World (initWorld)
import DA.Daml.LF.Simplifier (simplifyModule)


main :: IO ()
main = defaultMain $ testGroup "DA.Daml.LF.Simplifier"
    [ constantLiftingTests
    ]

constantLiftingTests :: TestTree
constantLiftingTests = testGroup "Constant Lifting"
    [ mkTestCase "empty module" [] []
    , mkTestCase "closed value"
        [ dval "foo" (TBuiltin BTInt64) (EBuiltin (BEInt64 10)) ]
        [ dval "foo" (TBuiltin BTInt64) (EBuiltin (BEInt64 10)) ]
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
        , dvalNoPartyLiterals = HasNoPartyLiterals True
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
            }
    version = versionDev
    world = initWorld [] version

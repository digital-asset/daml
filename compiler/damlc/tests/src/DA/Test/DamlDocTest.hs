-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlDocTest (main) where

import qualified Data.Text.Extended as T
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.DocTest
import DA.Daml.Options.Types
import DA.Daml.LF.Ast.Version (versionDev)
import qualified DA.Service.Logger.Impl.Pure as Logger
import DA.Test.DamlcIntegration (withDamlScriptDep, ScriptPackageData)
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules
import Development.IDE.Core.Service
import Development.IDE.Core.Shake
import Development.IDE.Types.Location

main :: IO ()
main = withDamlScriptDep versionDev $ \scriptPackageData -> -- Install Daml.Script once at the start of the suite, rather than for each case
    defaultMain $ testGroup "daml-doctest"
        [ generateTests scriptPackageData
        ]

-- These test names are converted to module names by removing spaces
-- Do not use any characters that wouldn't be accepted as a haskell module name (e.g. '-', '.', etc.)
generateTests :: ScriptPackageData -> TestTree
generateTests scriptPackageData = testGroup "generate doctest module"
    [ shouldGenerateCase "empty module" [] []
    , shouldGenerateCase "example in doc comment"
          [ "-- |"
          , "-- >>> 1 + 1"
          , "-- 2"
          ]
          [ "doctest_0 = script do"
          , "  (===) (1 + 1) $"
          , "     2"
          ]
    , shouldGenerateCase "example in nondoc comment"
          [ "-- >>> 1 + 1"
          , "-- 2"
          ]
          []
    , shouldGenerateCase "multiple examples in one comment"
          [ "-- |"
          , "-- >>> 1 + 1"
          , "-- 2"
          , "-- >>> 2 + 2"
          , "-- 4"
          ]
          [ "doctest_0 = script do"
          , "  (===) (1 + 1) $"
          , "     2"
          , ""
          , "doctest_1 = script do"
          , "  (===) (2 + 2) $"
          , "     4"
          ]
    , shouldGenerateCase "example in code block"
          [ "-- |"
          , "-- ```"
          , "-- >>> 1 + 1"
          , "-- 2"
          , "-- ```"
          ]
          [ "doctest_0 = script do"
          , "  (===) (1 + 1) $"
          , "     2"
          ]
    , shouldGenerateCase "multiline result"
          [ "-- |"
          , "-- >>> map (+1) [1,2,3]"
          , "-- [ 2"
          , "-- , 3"
          , "-- , 4"
          , "-- ]"
          ]
          [ "doctest_0 = script do"
          , "  (===) (map (+1) [1,2,3]) $"
          , "     [ 2"
          , "     , 3"
          , "     , 4"
          , "     ]"
          ]
    ]
    where
        shouldGenerateCase :: T.Text -> [T.Text] -> [T.Text] -> TestTree
        shouldGenerateCase name input expected = testCase (T.unpack name) $ do
            let moduleName = "Case_" <> T.replace " " "" name
                tmpFile = T.unpack moduleName <> ".daml"
            T.writeFileUtf8 tmpFile $ T.unlines $ testModuleHeader moduleName <> input
            let opts = (defaultOptions (Just versionDev))
                    { optHaddock = Haddock True
                    , optScenarioService = EnableScenarioService False
                    , optPackageDbs = [fst scriptPackageData]
                    , optPackageImports = [snd scriptPackageData]
                    }
            withDamlIdeState opts Logger.makeNopHandle (NotificationHandler $ \_ _ -> pure ()) $ \ideState -> do
                Just pm <- runActionSync ideState $ use GetParsedModule $ toNormalizedFilePath' tmpFile
                genModuleContent (getDocTestModule pm) @?= T.unlines (doctestHeader moduleName <> expected)

testModuleHeader :: T.Text -> [T.Text]
testModuleHeader moduleName =
    [ "module " <> moduleName <> " where"
    ]

doctestHeader :: T.Text -> [T.Text]
doctestHeader moduleName =
    [ "{-# OPTIONS_GHC -Wno-unused-imports #-}"
    , "module " <> moduleName <> "_doctest where"
    , ""
    , "import " <> moduleName
    , "import DA.Assert"
    , "import Daml.Script"
    , ""
    ]


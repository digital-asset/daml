-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlDocTest (main) where

import qualified Data.Text.Extended as T
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.DocTest
import DA.Daml.Options.Types
import DA.Daml.Options
import Development.IDE.Core.FileStore
import Development.IDE.Core.Rules
import Development.IDE.Core.Service
import Development.IDE.Core.Shake
import Development.IDE.Types.Location
import Development.IDE.Types.Logger
import Development.IDE.Types.Options

main :: IO ()
main = defaultMain $ testGroup "daml-doctest"
    [ generateTests
    ]

generateTests :: TestTree
generateTests = testGroup "generate doctest module"
    [ testCase "empty module" $
          [] `shouldGenerate` []
    , testCase "example in doc comment" $
          [ "-- |"
          , "-- >>> 1 + 1"
          , "-- 2"
          ] `shouldGenerate`
          [ "doctest_0 = scenario do"
          , "  (===) (1 + 1) $"
          , "     2"
          ]
    , testCase "example in non-doc comment" $
          [ "-- >>> 1 + 1"
          , "-- 2"
          ] `shouldGenerate`
          []
    , testCase "multiple examples in one comment" $
          [ "-- |"
          , "-- >>> 1 + 1"
          , "-- 2"
          , "-- >>> 2 + 2"
          , "-- 4"
          ] `shouldGenerate`
          [ "doctest_0 = scenario do"
          , "  (===) (1 + 1) $"
          , "     2"
          , ""
          , "doctest_1 = scenario do"
          , "  (===) (2 + 2) $"
          , "     4"
          ]
    , testCase "example in code block" $
          [ "-- |"
          , "-- ```"
          , "-- >>> 1 + 1"
          , "-- 2"
          , "-- ```"
          ] `shouldGenerate`
          [ "doctest_0 = scenario do"
          , "  (===) (1 + 1) $"
          , "     2"
          ]
    , testCase "multiline result" $
          [ "-- |"
          , "-- >>> map (+1) [1,2,3]"
          , "-- [ 2"
          , "-- , 3"
          , "-- , 4"
          , "-- ]"
          ] `shouldGenerate`
          [ "doctest_0 = scenario do"
          , "  (===) (map (+1) [1,2,3]) $"
          , "     [ 2"
          , "     , 3"
          , "     , 4"
          , "     ]"
          ]
    ]

testModuleHeader :: [T.Text]
testModuleHeader =
    [ "daml 1.2"
    , "module Test where"
    ]

doctestHeader :: [T.Text]
doctestHeader =
    [ "{-# OPTIONS_GHC -Wno-unused-imports #-}"
    , "daml 1.2"
    , "module Test_doctest where"
    , ""
    , "import Test"
    , "import DA.Assert"
    , ""
    ]

shouldGenerate :: [T.Text] -> [T.Text] -> Assertion
shouldGenerate input expected = withTempFile $ \tmpFile -> do
    T.writeFileUtf8 tmpFile $ T.unlines $ testModuleHeader <> input
    opts <- fmap (\opts -> opts{optHaddock=Haddock True}) $ defaultOptionsIO Nothing
    vfs <- makeVFSHandle
    ideState <- initialise mainRule (const $ pure ()) noLogging (toCompileOpts opts (IdeReportProgress False)) vfs
    Just pm <- runAction ideState $ use GetParsedModule $ toNormalizedFilePath tmpFile
    genModuleContent (getDocTestModule pm) @?= T.unlines (doctestHeader <> expected)

-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlcUpgrades (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.Foldable
import System.Directory.Extra
import System.FilePath
import System.IO.Extra
import DA.Test.Process
import Test.Tasty
import Test.Tasty.HUnit
import SdkVersion
import DA.Daml.LF.Ast.Version
import Text.Regex.TDFA
import qualified Data.Text as T

main :: IO ()
main = do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: FilePath -> TestTree
tests damlc =
    testGroup
        "Upgrade"
        [ test
              "Fails when new field is added without Optional type"
              (Just "Message: \n\ESC\\[0;91merror type checking data type MyLib.A:\n  EUpgradeRecordFieldsNewNonOptional")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  , new : Int"
                      , "  }"
                      ]
                )
              ]
        , test
              "Fails when old field is deleted"
              (Just "Message: \n\ESC\\[0;91merror type checking data type MyLib.A:\n  EUpgradeRecordFieldsMissing")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing2 : Int"
                      , "  }"
                      ]
                )
              ]
        , test
              "Fails when existing field is changed"
              (Just "Message: \n\ESC\\[0;91merror type checking data type MyLib.A:\n  EUpgradeRecordFieldsExistingChanged")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Text"
                      , "  , existing2 : Int"
                      , "  }"
                      ]
                )
              ]
        , test
              "Succeeds when new field is added with optional type"
              Nothing
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A"
                      , "  { existing1 : Int"
                      , "  , existing2 : Int"
                      , "  , new : Optional Int"
                      , "  }"
                      ]
                )
              ]
        ]
  where
    test ::
           String
        -> Maybe T.Text
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> TestTree
    test name expectedError oldVersion newVersion =
        testCase name $
        withTempDir $ \dir -> do
            let depDir = dir </> "oldVersion"
            let dar = dir </> "out.dar"
            let depDar = dir </> "oldVersion" </> "dep.dar"
            writeFiles dir (projectFile "mylib-v2" (Just depDar) : newVersion)
            writeFiles depDir (projectFile "mylib-v1" Nothing : oldVersion)
            callProcessSilent damlc ["build", "--project-root", depDir, "-o", depDar]
            case expectedError of
              Nothing ->
                  callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
              Just regex -> do
                  stderr <- callProcessForStderr damlc ["build", "--project-root", dir, "-o", dar]
                  unless (matchTest (makeRegex regex :: Regex) stderr) $
                      assertFailure ("Regex '" <> show regex <> "' did not match stderr:\n" <> show stderr)

    writeFiles dir fs =
        for_ fs $ \(file, content) -> do
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

    projectFile name upgradedFile =
        ( "daml.yaml"
        , unlines $
          [ "sdk-version: " <> sdkVersion
          , "name: " <> name
          , "source: daml"
          , "version: 0.0.1"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "typecheck-upgrades: true"
          , "build-options:"
          , "- --target=" <> renderVersion (featureMinVersion featurePackageUpgrades)
          ] ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
        )

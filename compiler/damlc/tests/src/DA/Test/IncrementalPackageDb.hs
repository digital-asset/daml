-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.IncrementalPackageDb (main) where

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.Foldable
import System.Directory.Extra
import System.FilePath
import System.IO.Extra
import DA.Test.Process
import Test.Tasty
import Test.Tasty.HUnit
import DA.Daml.Options.Packaging.Metadata (metadataFile)
import Development.IDE.Types.Location
import SdkVersion

newtype ExpectReinitialization = ExpectReinitialization Bool

main :: IO ()
main = do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: FilePath -> TestTree
tests damlc =
    testGroup
        "Incremental package db initialization"
        [ test
              "Reinitialize when dependency changes"
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      , "  - dependency/dep.dar"
                      ])
              , ("daml/A.daml"
                , unlines
                      [ "module A where"
                      , "import B"
                      , "x : Int"
                      , "x = f 1"])
              ]
              []
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              , ( "daml/B.daml"
                , unlines
                      [ "module B where"
                      , "template T"
                      , "  with"
                      , "    p: Party"
                      , "  where"
                      , "    signatory p"
                      , "f : Int -> Int"
                      , "f i = i + 1"
                      ])
              ]
              [ ( "daml/B.daml"
                , unlines
                      [ "module B where"
                      , "template T"
                      , "  with"
                      , "    p: Party"
                      , "  where"
                      , "    signatory p"
                      , "f : Int -> Int"
                      , "f i = i + 2"
                      ])
              ]
              (ExpectReinitialization True)
              (ShouldSucceed True)
        , test
              "Reinitialize when dependency is added"
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              , ("daml/A.daml", unlines ["module A where"])
              ]
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      , "  - dependency/dep.dar"
                      ])
              ]
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              , ( "daml/B.daml"
                , unlines
                      [ "module B where"
                      , "template T"
                      , "  with"
                      , "    p: Party"
                      , "  where"
                      , "    signatory p"
                      , "f : Int -> Int"
                      , "f i = i + 1"
                      ])
              ]
              []
              (ExpectReinitialization True)
              (ShouldSucceed True)
        , test
              "Fail when dependency is removed"
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      , "  - dependency/dep.dar"
                      ])
              , ("daml/A.daml"
                , unlines
                      ["module A where"
                      , "import B"
                      , "x : Int"
                      , "x = f 1"])
              ]
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              ]
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              , ( "daml/B.daml"
                , unlines
                      [ "module B where"
                      , "template T"
                      , "  with"
                      , "    p: Party"
                      , "  where"
                      , "    signatory p"
                      , "f : Int -> Int"
                      , "f i = i + 1"
                      ])
              ]
              []
              (ExpectReinitialization True)
              (ShouldSucceed False)
        , test
              "No reinitialization when nothing changes"
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      , "  - dependency/dep.dar"
                      ])
              , ("daml/A.daml"
                , unlines
                      ["module A where"
                      , "import B"])
              ]
              []
              [ ( "daml.yaml"
                , unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep"
                      , "source: daml"
                      , "version: 0.0.1"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ])
              , ( "daml/B.daml"
                , unlines
                      [ "module B where"
                      , "template T"
                      , "  with"
                      , "    p: Party"
                      , "  where"
                      , "    signatory p"
                      , "f : Int -> Int"
                      , "f i = i + 1"
                      ])
              ]
              []
              (ExpectReinitialization False)
              (ShouldSucceed True)
        ]
  where
    test ::
           String
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> ExpectReinitialization
        -> ShouldSucceed
        -> TestTree
    test name proj projModification dependency dependencyModification (ExpectReinitialization expectReinitialization) (ShouldSucceed shouldSucceed) =
        testCase name $
        withTempDir $ \dir -> do
            let depDir = dir </> "dependency"
            let metaFp = metadataFile $ toNormalizedFilePath' dir
            let dar = dir </> "out.dar"
            let depDar = dir </> "dependency" </> "dep.dar"
            writeFiles dir proj
            writeFiles depDir dependency
            callProcessSilent damlc ["build", "--project-root", depDir, "-o", depDar]
            callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
            metaModTime <- getModificationTime metaFp
            writeFiles dir projModification
            unless (null dependencyModification) $ do
                writeFiles depDir dependencyModification
                callProcessSilent damlc ["build", "--project-root", depDir, "-o", depDar]
            if shouldSucceed
                then callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
                else callProcessSilentError damlc ["build", "--project-root", dir, "-o", dar]
            newMetaModTime <- getModificationTime metaFp
            when expectReinitialization $
                assertBool "package-db was not re-initialized" $ newMetaModTime /= metaModTime
    writeFiles dir fs =
        for_ fs $ \(file, content) -> do
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

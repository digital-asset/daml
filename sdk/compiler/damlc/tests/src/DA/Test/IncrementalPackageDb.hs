-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.IncrementalPackageDb (main) where

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
import DA.Daml.Options.Packaging.Metadata (metadataFile)
import Development.IDE.Types.Location
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)

newtype ExpectReinitialization = ExpectReinitialization Bool

main :: IO ()
main = withSdkVersions $ do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: SdkVersioned => FilePath -> TestTree
tests damlc =
    testGroup
        "Incremental package db initialization"
        [ test
              "Reinitialize when dependency changes"
              [ packageDamlYaml "proj" ["dependency/dep.dar"]
              , ("daml/A.daml"
                , unlines
                      [ "module A where"
                      , "import B"
                      , "x : Int"
                      , "x = f 1"])
              ]
              [("daml/A.daml"
                , unlines
                      [ "module A where"
                      , "import B"
                      , "x : Int"
                      , "x = f 1 + g 2"])
              ]
              [ packageDamlYaml "dep" []
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
                      , "g : Int -> Int"
                      , "g i = i + 3"
                      ])
              ]
              (ExpectReinitialization True)
              (ShouldSucceed True)
        , test
              "Reinitialize when dependency is added"
              [ packageDamlYaml "proj" []
              , ("daml/A.daml", unlines [ "module A where"])
              ]
              [ packageDamlYaml "proj" ["dependency/dep.dar"]
              , ("daml/A.daml", unlines
                    [ "module A where"
                    , "import B"
                    , "g x = f x + 1"
                    ])
              ]
              [ packageDamlYaml "dep" []
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
              [ packageDamlYaml "proj" ["dependency/dep.dar"]
              , ("daml/A.daml"
                , unlines
                      ["module A where"
                      , "import B"
                      , "x : Int"
                      , "x = f 1"])
              ]
              [ packageDamlYaml "proj" []
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
              [ packageDamlYaml "proj" ["dependency/dep.dar"]
              , ("daml/A.daml"
                , unlines
                      ["module A where"
                      , "import B"])
              ]
              []
              [ packageDamlYaml "dep" []
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
            callProcessSilent damlc ["build", "--package-root", depDir, "-o", depDar]
            callProcessSilent damlc ["build", "--package-root", dir, "-o", dar]
            metaModTime <- getModificationTime metaFp
            writeFiles dir projModification
            unless (null dependencyModification) $ do
                writeFiles depDir dependencyModification
                callProcessSilent damlc ["build", "--package-root", depDir, "-o", depDar]
            if shouldSucceed
                then callProcessSilent damlc ["build", "--package-root", dir, "-o", dar]
                else callProcessSilentError damlc ["build", "--package-root", dir, "-o", dar]
            newMetaModTime <- getModificationTime metaFp
            when expectReinitialization $
                assertBool "package-db was not re-initialized" $ newMetaModTime /= metaModTime
    writeFiles dir fs =
        for_ fs $ \(file, content) -> do
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content
    packageDamlYaml name deps =
        ( "daml.yaml"
        , unlines $
          [ "sdk-version: " <> sdkVersion
          , "name: " <> name
          , "source: daml"
          , "version: 0.0.1"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ] ++
          ["  - " <> dep | dep <- deps])

-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- cache-reset: 1

module DamlcVisualize (main) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Either.Combinators
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Proto3.Archive as Archive
import Development.IDE.Core.API.Testing.Visualize
import DA.Daml.LF.Ast
    ( ExternalPackage(..)
    , World
    , initWorldSelf
    )
import DA.Daml.LF.Reader (Dalfs(..), readDalfs)
import DA.Test.Process
import SdkVersion
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ testGroup "visualize"
        [ testCase "single package" $ withTempDir $ \dir -> do
              writeFileUTF8 (dir </> "daml.yaml") $ unlines
                  [ "sdk-version: " <> sdkVersion
                  , "name: foobar"
                  , "source: ."
                  , "version: 0.0.1"
                  , "dependencies: [daml-stdlib, daml-prim]"
                  ]
              writeFileUTF8 (dir </> "A.daml") $ unlines
                 [ "module A where"
                 , "import B"
                 , "template A with"
                 , "    p : Party"
                 , "  where"
                 , "    signatory p"
                 , "    choice CreateB : ContractId B"
                 , "      controller p"
                 , "      do create B with p"
                 ]
              writeFileUTF8 (dir </> "B.daml") $ unlines
                 [ "module B where"
                 , "template B with"
                 , "    p : Party"
                 , "  where"
                 , "    signatory p"
                 ]
              withCurrentDirectory dir $ callProcessSilent damlc ["build", "-o", "foobar.dar"]
              testFile (dir </> "foobar.dar") ExpectedGraph
                  { expectedSubgraphs =
                        [ ExpectedSubGraph
                              { expectedNodes = ["Create", "Archive"]
                              , expectedTplFields = ["p"]
                              , expectedTemplate = "B"
                              }
                        , ExpectedSubGraph
                              { expectedNodes = ["Create", "Archive", "CreateB"]
                              , expectedTplFields = ["p"]
                              , expectedTemplate = "A"
                              }
                        ]
                  , expectedEdges =
                        [ ( ExpectedChoiceDetails
                                { expectedConsuming = True
                                , expectedName = "CreateB"
                                }
                          , ExpectedChoiceDetails
                               { expectedConsuming = False
                               , expectedName = "Create"
                               }
                          )
                        ]
                  }
        , multiPackageTests damlc
        ]

multiPackageTests :: FilePath -> TestTree
multiPackageTests damlc = testGroup "multiple packages"
    [ testCase "different module names" $ withTempDir $ \dir -> do
          createDirectory (dir </> "foobar-a")
          createDirectory (dir </> "foobar-b")
          writeFileUTF8 (dir </> "foobar-b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: foobar-b"
              , "source: ."
              , "version: 0.0.1"
              , "dependencies: [daml-stdlib, daml-prim]"
              ]
          writeFileUTF8 (dir </> "foobar-b" </> "B.daml") $ unlines
             [ "module B where"
             , "template B with"
             , "    p : Party"
             , "  where"
             , "    signatory p"
             ]
          withCurrentDirectory (dir </> "foobar-b") $ callProcessSilent damlc ["build", "-o", "foobar-b.dar"]
          writeFileUTF8 (dir </> "foobar-a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: foobar-a"
              , "source: ."
              , "version: 0.0.1"
              , "dependencies: [daml-stdlib, daml-prim, " <>
                show (dir </> "foobar-b" </> "foobar-b.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "foobar-a" </> "A.daml") $ unlines
             [ "module A where"
             , "import B"
             , "template A with"
             , "    p : Party"
             , "  where"
             , "    signatory p"
             , "    choice CreateB : ContractId B"
             , "      controller p"
             , "      do create B with p"
             ]
          withCurrentDirectory (dir </> "foobar-a") $ callProcessSilent damlc ["build", "-o", "foobar-a.dar"]
          testFile (dir </> "foobar-a" </> "foobar-a.dar") ExpectedGraph
              { expectedSubgraphs =
                    [ ExpectedSubGraph
                          { expectedNodes = ["Create", "Archive", "CreateB"]
                          , expectedTplFields = ["p"]
                          , expectedTemplate = "A"
                          }
                    , ExpectedSubGraph
                          { expectedNodes = ["Create", "Archive"]
                          , expectedTplFields = ["p"]
                          , expectedTemplate = "B"
                          }
                    ]
              , expectedEdges =
                    [ ( ExpectedChoiceDetails
                            { expectedConsuming = True
                            , expectedName = "CreateB"
                            }
                      , ExpectedChoiceDetails
                           { expectedConsuming = False
                           , expectedName = "Create"
                           }
                      )
                    ]
              }
    , testCase "same module names" $ withTempDir $ \dir -> do
          createDirectory (dir </> "foobar-a")
          createDirectory (dir </> "foobar-b")
          writeFileUTF8 (dir </> "foobar-b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: foobar-b"
              , "source: ."
              , "version: 0.0.1"
              , "dependencies: [daml-stdlib, daml-prim]"
              ]
          writeFileUTF8 (dir </> "foobar-b" </> "A.daml") $ unlines
             [ "module A where"
             , "template T with"
             , "    p : Party"
             , "  where"
             , "    signatory p"
             ]
          withCurrentDirectory (dir </> "foobar-b") $ callProcessSilent damlc ["build", "-o", "foobar-b.dar"]
          writeFileUTF8 (dir </> "foobar-a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: foobar-a"
              , "source: ."
              , "version: 0.0.1"
              , "dependencies: [daml-stdlib, daml-prim, " <>
                show (dir </> "foobar-b" </> "foobar-b.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "foobar-a" </> "A.daml") $ unlines
             [ "module A where"
             , "import qualified \"foobar-b\" A as AA"
             , "template T with"
             , "    p : Party"
             , "  where"
             , "    signatory p"
             , "    choice CreateB : ContractId AA.T"
             , "      controller p"
             , "      do create AA.T with p"
             ]
          withCurrentDirectory (dir </> "foobar-a") $ callProcessSilent damlc ["build", "-o", "foobar-a.dar"]
          testFile (dir </> "foobar-a" </> "foobar-a.dar") ExpectedGraph
              { expectedSubgraphs =
                    [ ExpectedSubGraph
                          { expectedNodes = ["Create", "Archive", "CreateB"]
                          , expectedTplFields = ["p"]
                          , expectedTemplate = "T"
                          }
                    , ExpectedSubGraph
                          { expectedNodes = ["Create", "Archive"]
                          , expectedTplFields = ["p"]
                          , expectedTemplate = "T"
                          }
                    ]
              , expectedEdges =
                    [ ( ExpectedChoiceDetails
                            { expectedConsuming = True
                            , expectedName = "CreateB"
                            }
                      , ExpectedChoiceDetails
                           { expectedConsuming = False
                           , expectedName = "Create"
                           }
                      )
                    ]
              }
    ]

testFile :: FilePath -> ExpectedGraph -> Assertion
testFile dar expected = do
    darBytes <- BS.readFile dar
    dalfs <- either fail pure $ readDalfs $ Zip.toArchive (BSL.fromStrict darBytes)
    !world <- pure $ darToWorld dalfs
    whenLeft (graphTest world expected) $
        \(FailedGraphExpectation expected actual) ->
        assertFailure $ unlines
           [ "Failed graph expectation:"
           , "Expected:"
           , show expected
           , "Actual:"
           , show actual
           ]

dalfBytesToPakage :: BSL.ByteString -> ExternalPackage
dalfBytesToPakage bytes = case Archive.decodeArchive Archive.DecodeAsDependency $ BSL.toStrict bytes of
    Right (pkgId, pkg) -> ExternalPackage pkgId pkg
    Left err -> error (show err)

darToWorld :: Dalfs -> World
darToWorld Dalfs{..} = case Archive.decodeArchive Archive.DecodeAsMain $ BSL.toStrict mainDalf of
    Right (_, mainPkg) -> initWorldSelf pkgs mainPkg
    Left err -> error (show err)
    where
        pkgs = map dalfBytesToPakage dalfs


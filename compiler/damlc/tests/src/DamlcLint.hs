-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcLint
   ( main
   ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Data.List.Extra (isSuffixOf)
import System.Environment.Blank
import System.Directory
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import qualified Data.Text.Extended as T

import DA.Bazel.Runfiles
import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain (tests damlc)

tests :: FilePath -> TestTree
tests damlc = testGroup "damlc" $ map (\f -> f damlc)
  [ testsForDamlcLint
  ]

testsForDamlcLint :: FilePath -> TestTree
testsForDamlcLint damlc = testGroup "damlc test"
    [ testCase "Lint all project files" $ do
        withTempDir $ \dir -> do
          withCurrentDirectory dir $ do
            let projDir = "daml"
            createDirectoryIfMissing True $ dir </> projDir
            let damlFile = dir </> "daml.yaml"
            T.writeFileUtf8 damlFile $ T.unlines
              [ "sdk-version: " <> T.pack sdkVersion
              , "name: a"
              , "version: 0.0.1"
              , "source: " <> T.pack projDir
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
            let fooFile = dir </> projDir </> "Foo.daml"
            T.writeFileUtf8 fooFile $ T.unlines
              [ "module Foo where"
              , "template S with p : Party where"
              , "  signatory p"
              , "  choice X : Int with"
              , "    controller p"
              , "      do"
              , "         forA [] $ \\_ -> create S with p"
              , "         pure 42"
              ]
            let barFile = dir </> projDir </> "Bar.daml"
            T.writeFileUtf8 barFile $ T.unlines
              [ "module Bar where"
              , "template T with p : Party where"
              , "  signatory p"
              ]
            (exitCode, _stdout, stderr) <-
              readProcessWithExitCode damlc ["lint"] ""
            exitCode @?= ExitFailure 1
            assertBool
                ("All project source files are linted: " <> show stderr)
                (unlines
                     [ "Found:"
                     , "  forA [] $ \\ _ -> create S {p}"
                     , "Perhaps:"
                     , "  forA_ [] $ \\ _ -> create S {p}\n\ESC[0m"
                     ] `isSuffixOf` stderr) -- needs escape secenquence for blue output color.
    , testCase "Lint all project files -- no hints" $ do
        withTempDir $ \dir -> do
          withCurrentDirectory dir $ do
            let projDir = "daml"
            createDirectoryIfMissing True $ dir </> projDir
            let damlFile = dir </> "daml.yaml"
            T.writeFileUtf8 damlFile $ T.unlines
              [ "sdk-version: " <> T.pack sdkVersion
              , "name: a"
              , "version: 0.0.1"
              , "source: " <> T.pack projDir
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
            let fooFile = dir </> projDir </> "Foo.daml"
            T.writeFileUtf8 fooFile $ T.unlines
              [ "module Foo where"
              , "template S with p : Party where"
              , "  signatory p"
              ]
            let barFile = dir </> projDir </> "Bar.daml"
            T.writeFileUtf8 barFile $ T.unlines
              [ "module Bar where"
              , "template T with p : Party where"
              , "  signatory p"
              ]
            (exitCode, _stdout, stderr) <-
              readProcessWithExitCode damlc ["lint"] ""
            exitCode @?= ExitSuccess
            assertBool
                ("All project source files are linted: " <> show stderr)
                (stderr == "No hints\n")
    ]

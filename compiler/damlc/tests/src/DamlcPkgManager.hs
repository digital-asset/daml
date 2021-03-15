-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcPkgManager
   ( main
   ) where

import DA.Bazel.Runfiles
import DA.Cli.Damlc.InspectDar
import qualified DA.Daml.LF.Ast as LF
import DA.Test.Sandbox
import qualified Data.Text.Extended as T
import SdkVersion
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    dar <-
        locateRunfiles
            (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "pkg-manager-test.dar")
    defaultMain (tests damlc dar)

tests :: FilePath -> FilePath -> TestTree
tests damlc dar =
    testGroup "damlc package manager" $ map (\f -> f damlc dar) [testsForRemoteDataDependencies]

testsForRemoteDataDependencies :: FilePath -> FilePath -> TestTree
testsForRemoteDataDependencies damlc dar =
    withSandbox defaultSandboxConf {dars = [dar]} $ \getSandboxPort -> do
        testGroup "Remote dependencies"
            [ testCase "Package id data-dependency" $
              withTempDir $ \projDir -> do
                  InspectInfo {mainPackageId} <- getDarInfo dar
                  let mainPkgId = T.unpack $ LF.unPackageId mainPackageId
                  sandboxPort <- getSandboxPort
                  writeFileUTF8 (projDir </> "daml.yaml") $
                      unlines
                          [ "sdk-version: " <> sdkVersion
                          , "name: a"
                          , "version: 0.0.1"
                          , "source: ."
                          , "dependencies: [daml-prim, daml-stdlib]"
                          , "data-dependencies: [ " ++ mainPkgId ++ "]"
                          , "ledger:"
                          , "  host: localhost"
                          , "  port: " <> show sandboxPort
                          ]
                  writeFileUTF8 (projDir </> "A.daml") $
                      unlines
                          [ "module A where"
                          , "import PkgManagerTest (S)"
                          , "type U = S"
                          , "template T with p : Party where"
                          , "  signatory p"
                          ]
                  setEnv "DAML_PROJECT" projDir True
                  (exitCode, _stdout, stderr) <-
                      readProcessWithExitCode damlc ["build", "--project-root", projDir] ""
                  stderr @?= ""
                  exitCode @?= ExitSuccess
            ]

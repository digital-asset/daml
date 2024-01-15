-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcPkgManager
    ( main
    ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Bazel.Runfiles
import DA.Daml.Dar.Reader
import DA.Daml.Helper.Ledger (downloadAllReachablePackages)
import qualified DA.Daml.LF.Ast as LF
import DA.Test.Sandbox
import qualified Data.HashMap.Strict as HMS
import Data.List
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text.Extended as T
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)
import System.Directory
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = withSdkVersions $ do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    dar <-
        locateRunfiles
            (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "pkg-manager-test.dar")
    defaultMain (tests damlc dar)

tests :: SdkVersioned => FilePath -> FilePath -> TestTree
tests damlc dar =
    testGroup "damlc package manager" $ map (\f -> f damlc dar) [testsForRemoteDataDependencies]

testsForRemoteDataDependencies :: SdkVersioned => FilePath -> FilePath -> TestTree
testsForRemoteDataDependencies damlc dar =
    testGroup "Remote dependencies"
    [ withCantonSandbox defaultSandboxConf {dars = [dar]} $ \getSandboxPort -> do
          testGroup
              "un-authenticated"
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
                            , "data-dependencies: [" ++ mainPkgId ++ "]"
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
                    (exitCode, _stdout, _stderr) <- readProcessWithExitCode damlc ["build"] ""
                    exitCode @?= ExitSuccess
              , testCase "package name:version data-dependency" $
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
                            , "data-dependencies: [pkg-manager-test:1.0.0]"
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
                    (exitCode, _stdout, _stderr) <- readProcessWithExitCode damlc ["build"] ""
                    exitCode @?= ExitSuccess
                  -- check that a lock file is written
                    let lockFp = projDir </> "daml.lock"
                    hasLockFile <- doesFileExist lockFp
                    hasLockFile @?= True
                    removeFile lockFp
                  -- check that a lock file is updated when a missing dependency is encountered.
                    writeFileUTF8 lockFp $
                        unlines
                            [ "dependencies:"
                            , "- pkgId: 51255efad65a1751bcee749d962a135a65d12b87eb81ac961142196d8bbca535"
                            , "  name: foo"
                            , "  version: 0.0.1"
                            ]
                    removeDirectoryRecursive $ projDir </> ".daml"
                    (exitCode, _stdout, _stderr) <- readProcessWithExitCode damlc ["build"] ""
                    exitCode @?= ExitSuccess
                    lock <- readFile lockFp
                    lines lock @?=
                        [ "dependencies:"
                        , "- name: pkg-manager-test"
                        , "  pkgId: " <> mainPkgId
                        , "  version: 1.0.0"
                        ]
                  -- a second call shouldn't make any network connections. So let's set the sandbox
                  -- port to a closed one.
                    writeFileUTF8 (projDir </> "daml.yaml") $
                        unlines
                            [ "sdk-version: " <> sdkVersion
                            , "name: a"
                            , "version: 0.0.1"
                            , "source: ."
                            , "dependencies: [daml-prim, daml-stdlib]"
                            , "data-dependencies: [pkg-manager-test:1.0.0]"
                            , "ledger:"
                            , "  host: localhost"
                            , "  port: " <> show (sandboxPort + 1)
                            ]
                    (exitCode, _stdout, _stderr) <- readProcessWithExitCode damlc ["build"] ""
                    exitCode @?= ExitSuccess
              , testCase "Caching" $ do
                    InspectInfo {mainPackageId, packages} <- getDarInfo dar
                    let downloadPkg pkgId
                          = do let DalfInfo {dalfPackage}
                                     = fromMaybe (error "DamlcPkgManager: can't find package id")
                                         $ HMS.lookup pkgId packages
                               pure dalfPackage
                    pkgs <- downloadAllReachablePackages downloadPkg [mainPackageId] []
                    (all isJust $ M.elems pkgs) @?= True
                  -- all packages need to be downloaded
                    let pkgIds = delete mainPackageId $ M.keys pkgs
                    pkgs1 <- downloadAllReachablePackages downloadPkg [mainPackageId] pkgIds
                    (all isNothing $ M.elems $ M.delete mainPackageId pkgs1) @?= True
                  -- only the main package needs to be downloaded, while the direct dependencies are
                  -- already present.
              ]
    , withCantonSandbox defaultSandboxConf {mbSharedSecret = Just "secret", dars = [dar]} $ \getSandboxPort
    -- run the sandbox with authentication to check that we can access it given an authentication
    -- token.
       -> do
          testGroup
              "authenticated"
              [ testCase "Package id data-dependency" $
                withTempDir $ \projDir -> do
                    InspectInfo {mainPackageId} <- getDarInfo dar
                    let mainPkgId = T.unpack $ LF.unPackageId mainPackageId
                    let tokenFp = projDir </> "token"
                    writeFileUTF8 tokenFp $ makeSignedJwt "secret" []
                    sandboxPort <- getSandboxPort
                    writeFileUTF8 (projDir </> "daml.yaml") $
                        unlines
                            [ "sdk-version: " <> sdkVersion
                            , "name: a"
                            , "version: 0.0.1"
                            , "source: ."
                            , "dependencies: [daml-prim, daml-stdlib]"
                            , "data-dependencies: [" ++ mainPkgId ++ "]"
                            , "ledger:"
                            , "  host: localhost"
                            , "  port: " <> show sandboxPort
                            , "  access-token-file: " <> tokenFp
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
                    (exitCode, _stdout, _stderr) <- readProcessWithExitCode damlc ["build"] ""
                    exitCode @?= ExitSuccess
              ]
    ]

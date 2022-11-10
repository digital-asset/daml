-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Deployment (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception
import qualified Data.UUID.V4 as UUID
import System.Directory.Extra (withCurrentDirectory)
import System.Environment.Blank (setEnv, unsetEnv)
import System.Exit
import System.FilePath ((</>))
import System.IO.Extra (withTempDir,writeFileUTF8)
import System.Process
import Test.Tasty (TestTree,defaultMain,testGroup)
import Test.Tasty.HUnit
import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T

import DA.Bazel.Runfiles (mainWorkspace,locateRunfiles,exe)
import DA.Daml.LF.Reader (Dalfs(..),readDalfs)
import DA.Test.Process (callProcessSilent)
import DA.Test.Sandbox (mbSharedSecret,withSandbox,defaultSandboxConf, makeSignedJwt)
import DA.Test.Util
import SdkVersion (sdkVersion)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as LFArchive

data Tools = Tools { damlc :: FilePath, damlHelper :: FilePath }

-- NOTE: This test was moved to the compatibility tests in
-- `compatibility/bazel_tools/daml_ledger/Main.hs`. This file remains for now
-- for easier iterative testing during development. If you modify these tests
-- then please keep them in sync with the corresponding tests in the
-- compatibility workspace.

main :: IO ()
main = do
  -- We manipulate global state via the working directory
  -- so running tests in parallel will cause trouble.
  setEnv "TASTY_NUM_THREADS" "1" True
  damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
  damlHelper <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
  let tools = Tools {..}
  defaultMain $ testGroup "Deployment"
    [ authenticationTests tools
    , unauthenticatedTests tools
    ]

-- | Test `daml ledger list-parties --access-token-file`
authenticationTests :: Tools -> TestTree
authenticationTests Tools{..} =
  withSandbox defaultSandboxConf { mbSharedSecret = Just sharedSecret } $ \getSandboxPort ->
    testGroup "authentication"
    [ testCase "Bearer prefix" $ do
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile ("Bearer " <> makeSignedJwt sharedSecret [] <> "\n")
              callProcessSilent damlHelper
                [ "ledger", "list-parties"
                , "--access-token-file", tokenFile
                , "--host", "localhost", "--port", show port
                ]
    , testCase "no Bearer prefix" $ do
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile (makeSignedJwt sharedSecret [] <> "\n")
              callProcessSilent damlHelper
                [ "ledger", "list-parties"
                , "--access-token-file", tokenFile
                , "--host", "localhost", "--port", show port
                ]
    , testCase "ledger.access-token-file field in daml.yaml" $ do
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              writeMinimalProject
              let tokenFile = deployDir </> "secretToken.jwt"
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile (makeSignedJwt sharedSecret [] <> "\n")
              appendFile "daml.yaml" $ unlines
                ["ledger:"
                , "  access-token-file: " <> tokenFile
                ]
              writeFileUTF8 tokenFile (makeSignedJwt sharedSecret [] <> "\n")
              setEnv "DAML_PROJECT" deployDir True
              callProcessSilent damlHelper
                [ "ledger", "list-parties"
                , "--host", "localhost", "--port", show port
                ]
              unsetEnv "DAML_PROJECT"

    ]
  where
    sharedSecret = "TheSharedSecret"

unauthenticatedTests :: Tools -> TestTree
unauthenticatedTests tools = do
    withSandbox defaultSandboxConf $ \getSandboxPort ->
        testGroup "unauthenticated"
            [ fetchTest tools getSandboxPort
            , timeoutTest tools getSandboxPort
            ]

timeoutTest :: Tools -> IO Int -> TestTree
timeoutTest Tools{..} getSandboxPort = do
    testCase "timeout" $ do
        port <- getSandboxPort
        party <- show <$> UUID.nextRandom
        (exit, stdout, stderr) <- readProcessWithExitCode damlHelper
            [ "ledger", "allocate-party", party
            , "--host", "localhost"
            , "--port", show port
            , "--timeout", "0"
            ]
            ""
        -- Not quite sure when we get which error message but both are fine.
        assertInfixOf "GRPCIOTimeout" stderr `catch`
            \(_ :: HUnitFailure) -> assertInfixOf "StatusDeadlineExceeded" stderr
        assertInfixOf "Checking party allocation" stdout
        exit @?= ExitFailure 1

-- | Test `daml ledger fetch-dar`
fetchTest :: Tools -> IO Int -> TestTree
fetchTest Tools{..} getSandboxPort = do
    testCaseSteps "fetchTest" $ \step -> do
    port <- getSandboxPort
    withTempDir $ \fetchDir -> do
      withCurrentDirectory fetchDir $ do
        writeMinimalProject
        let origDar = ".daml/dist/proj1-0.0.1.dar"
        step "build/upload"
        callProcessSilent damlc ["build"]
        callProcessSilent damlHelper
          [ "ledger", "upload-dar"
          , "--host", "localhost" , "--port" , show port
          , "--timeout=120"
          , origDar
          ]
        pid <- getMainPidOfDar origDar
        step "fetch/validate"
        let fetchedDar = "fetched.dar"
        callProcessSilent damlHelper
          [ "ledger", "fetch-dar"
          , "--host", "localhost" , "--port", show port
          , "--main-package-id", pid
          , "--timeout=120"
          , "-o", fetchedDar
          ]
        callProcessSilent damlc ["validate-dar", fetchedDar]

-- | Discover the main package-identifier of a dar.
getMainPidOfDar :: FilePath -> IO String
getMainPidOfDar fp = do
  archive <- Zip.toArchive <$> BSL.readFile fp
  Dalfs mainDalf _ <- either fail pure $ readDalfs archive
  Right pkgId <- pure $ LFArchive.decodeArchivePackageId $ BSL.toStrict mainDalf
  return $ T.unpack $ LF.unPackageId pkgId

-- | Write `daml.yaml` and `Main.daml` files in the current directory.
writeMinimalProject :: IO ()
writeMinimalProject = do
  writeFileUTF8 "daml.yaml" $ unlines
      [ "sdk-version: " <> sdkVersion
      , "name: proj1"
      , "version: 0.0.1"
      , "source: ."
      , "dependencies:"
      , "  - daml-prim"
      , "  - daml-stdlib"
      ]
  writeFileUTF8 "Main.daml" $ unlines
    [ "module Main where"
    , "template T with p : Party where signatory p"
    ]

-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Deployment (main) where

import Data.List.Extra (isInfixOf,splitOn)
import System.Directory.Extra (withCurrentDirectory)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>))
import System.IO.Extra (withTempDir,writeFileUTF8)
import Test.Tasty (TestTree,defaultMain,testGroup)
import Test.Tasty.HUnit (testCaseSteps)
import qualified Data.Aeson as Aeson
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Web.JWT as JWT

import DA.Bazel.Runfiles (mainWorkspace,locateRunfiles,exe)
import DA.Test.Sandbox (mbSharedSecret,withSandbox,defaultSandboxConf)
import DA.Test.Process (callProcessSilent,callProcessForStdout)
import SdkVersion (sdkVersion)

data Tools = Tools { damlc :: FilePath, damlHelper :: FilePath }

main :: IO ()
main = do
  -- We manipulate global state via the working directory
  -- so running tests in parallel will cause trouble.
  setEnv "TASTY_NUM_THREADS" "1" True
  damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
  damlHelper <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
  let tools = Tools {..}
  defaultMain $ testGroup "Deployment"
    [ authenticatedUploadTest tools
    , fetchTest tools
    ]

-- | Test `daml ledger upload-dar --access-token-file`
authenticatedUploadTest :: Tools -> TestTree
authenticatedUploadTest Tools{..} = do
  let sharedSecret = "TheSharedSecret"
  withSandbox defaultSandboxConf { mbSharedSecret = Just sharedSecret } $ \getSandboxPort ->
    testCaseSteps "authenticatedUploadTest" $ \step -> do
    port <- getSandboxPort
    withTempDir $ \deployDir -> do
      withCurrentDirectory deployDir $ do
        writeMinimalProject
        step "build"
        callProcessSilent damlc ["build"]
        let dar = ".daml/dist/proj1-0.0.1.dar"
        let tokenFile = deployDir </> "secretToken.jwt"
        step "upload"
        -- The trailing newline is not required but we want to test that it is supported.
        writeFileUTF8 tokenFile ("Bearer " <> makeSignedJwt sharedSecret <> "\n")
        callProcessSilent damlHelper
          [ "ledger", "upload-dar"
          , "--access-token-file", tokenFile
          , "--host", "localhost", "--port", show port
          , dar
          ]

makeSignedJwt :: String -> String
makeSignedJwt sharedSecret = do
  let urc = JWT.ClaimsMap $ Map.fromList [ ("admin", Aeson.Bool True)]
  let cs = mempty { JWT.unregisteredClaims = urc }
  let key = JWT.hmacSecret $ T.pack sharedSecret
  let text = JWT.encodeSigned key mempty cs
  T.unpack text

-- | Test `daml ledger fetch-dar`
fetchTest :: Tools -> TestTree
fetchTest tools@Tools{..} = do
  withSandbox defaultSandboxConf $ \getSandboxPort ->
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
          , origDar
          ]
        pid <- getMainPidByInspecingDar tools origDar "proj1"
        step "fetch/validate"
        let fetchedDar = "fetched.dar"
        callProcessSilent damlHelper
          [ "ledger", "fetch-dar"
          , "--host", "localhost" , "--port", show port
          , "--main-package-id", pid
          , "-o", fetchedDar
          ]
        callProcessSilent damlc ["validate-dar", fetchedDar]

-- | Using `daml inspect-dar`, discover the main package-identifier of a dar.
getMainPidByInspecingDar :: Tools -> FilePath -> String -> IO String
getMainPidByInspecingDar Tools{damlc} dar projName = do
  stdout <- callProcessForStdout damlc ["inspect-dar", dar]
  [grepped] <- pure $
        [ line
        | line <- lines stdout
        -- expect a single line containing double quotes and the projName
        , "\"" `isInfixOf` line
        , projName `isInfixOf` line
        ]
  -- and the main pid is found between the 1st and 2nd double-quotes
  [_,pid,_] <- pure $ splitOn "\"" grepped
  return pid

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
    [ "daml 1.2"
    , "module Main where"
    , "template T with p : Party where signatory p"
    ]

-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.QuickstartTests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Conduit hiding (connect)
import Control.Concurrent
import qualified Data.ByteString.Lazy.Char8 as LBS8
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import Data.List.Extra
import Data.Maybe (maybeToList)
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket.Extended
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Daml.Helper.Util (waitForHttpServer)
import DA.Test.Process (callCommandSilent, callCommandSilentIn, callCommandSilentWithEnvIn)
import DA.Test.Util
import DA.PortFile

main :: IO ()
main = do
    args <- getArgs
    withTempDir $ \tmpDir -> do
        oldPath <- getSearchPath
        javaPath <- locateRunfiles "local_jdk/bin"
        mvnPath <- locateRunfiles "mvn_dev_env/bin"
        tarPath <- locateRunfiles "tar_dev_env/bin"
        -- NOTE(Sofia): We don't use `script` on Windows.
        mbScriptPath <- if isWindows
            then pure Nothing
            else Just <$> locateRunfiles "script_nix/bin"
        -- NOTE: `COMSPEC` env. variable on Windows points to cmd.exe, which is required to be present
        -- on the PATH as mvn.cmd executes cmd.exe
        mbComSpec <- getEnv "COMSPEC"
        let mbCmdDir = takeDirectory <$> mbComSpec
        limitJvmMemory defaultJvmMemoryLimits
        withArgs args (withEnv
            [ ("PATH", Just $ intercalate [searchPathSeparator] $ concat
                [ [tarPath, javaPath, mvnPath]
                , maybeToList mbScriptPath
                , oldPath
                , maybeToList mbCmdDir
                ])
            , ("TASTY_NUM_THREADS", Just "2")
            ] $ defaultMain (tests tmpDir))

data QuickSandboxResource = QuickSandboxResource
    { quickSandboxPort :: PortNumber
    , quickProjDir :: FilePath
    , quickDar :: FilePath
    , quickSandboxPh :: ProcessHandle
    }

quickSandbox :: FilePath -> IO QuickSandboxResource
quickSandbox projDir = do
    withDevNull $ \devNull -> do
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=quickstart-java"]
        callCommandSilentIn projDir "daml build"
        ports <- sandboxPorts
        let portFile = "portfile.json"
        let darFile = ".daml" </> "dist" </> "quickstart-0.0.1.dar"
        let sandboxProc =
                (shell $
                    unwords
                        [ "daml"
                        , "sandbox"
                        , "--port" , show $ ledger ports
                        , "--admin-api-port", show $ admin ports
                        , "--domain-public-port", show $ domainPublic ports
                        , "--domain-admin-port", show $ domainAdmin ports
                        , "--port-file", portFile
                        , "--dar", darFile
                        , "--static-time"
                        ])
                    {std_out = UseHandle devNull, create_group = True, cwd = Just projDir}
        (_, _, _, sandboxPh) <- createProcess sandboxProc
        _ <- readPortFile sandboxPh maxRetries (projDir </> portFile)
        pure $
            QuickSandboxResource
                { quickProjDir = projDir
                , quickSandboxPort = ledger ports
                , quickSandboxPh = sandboxPh
                , quickDar = projDir </> darFile
                }

tests :: FilePath -> TestTree
tests tmpDir =
    withSdkResource $ \_ ->
        testGroup
            "Quickstart tests"
            [ withResource (quickSandbox quickstartDir) (interruptProcessGroupOf . quickSandboxPh) $
              quickstartTests quickstartDir mvnDir
            ]
  where
    quickstartDir = tmpDir </> "q-u-i-c-k-s-t-a-r-t"
    mvnDir = tmpDir </> "m2"

quickstartTests :: FilePath -> FilePath -> IO QuickSandboxResource -> TestTree
quickstartTests quickstartDir mvnDir getSandbox =
    testCaseSteps "quickstart" $ \step -> do
        let subtest :: forall t. String -> IO t -> IO t
            subtest m p = step m >> p
        subtest "daml test" $
            callCommandSilentIn quickstartDir "daml test"
        -- Testing `daml new` and `daml build` is done when the QuickSandboxResource is build.
        subtest "daml damlc test --files" $
            callCommandSilentIn quickstartDir "daml damlc test --files daml/Main.daml"
        subtest "mvn compile" $ do
            mvnDbTarball <-
                locateRunfiles
                    (mainWorkspace </> "daml-assistant" </> "integration-tests" </>
                    "integration-tests-mvn.tar")
            runConduitRes $
                sourceFileBS mvnDbTarball .|
                Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError mvnDir)
            callCommandSilentIn quickstartDir "daml codegen java"
            callCommandSilentIn quickstartDir $ unwords ["mvn", mvnRepoFlag, "-q", "compile"]
        subtest "mvn exec:java@run-quickstart" $ do
            QuickSandboxResource {quickProjDir, quickSandboxPort, quickDar} <- getSandbox
            withDevNull $ \devNull -> do
                callCommandSilentIn quickProjDir $
                    unwords
                        [ "daml script"
                        , "--dar " <> quickDar
                        , "--script-name Main:initialize"
                        , "--static-time"
                        , "--ledger-host localhost"
                        , "--ledger-port"
                        , show quickSandboxPort
                        , "--output-file", "output.json"
                        ]
                scriptOutput <- readFileUTF8 (quickProjDir </> "output.json")
                [alice, eurBank] <- pure (read scriptOutput :: [String])
                take 7 alice @?= "Alice::"
                take 10 eurBank @?= "EUR_Bank::"
                drop 7 alice @?= drop 10 eurBank -- assert that namespaces are equal

                restPort :: Int <- fromIntegral <$> getFreePort
                let mavenProc = (shell $ unwords
                        [ "mvn"
                        , mvnRepoFlag
                        , "-Dledgerport=" <> show quickSandboxPort
                        , "-Drestport=" <> show restPort
                        , "-Dparty=" <> alice
                        , "exec:java@run-quickstart"
                        ])
                        { std_out = UseHandle devNull
                        , cwd = Just quickProjDir }
                withCreateProcess mavenProc $ \_ _ _ mavenPh -> do
                    let url = "http://localhost:" <> show restPort <> "/iou"
                    waitForHttpServer 240 mavenPh (threadDelay 500000) url []
                    threadDelay 5000000
                    manager <- newManager defaultManagerSettings
                    req <- parseRequest url
                    req <-
                        pure req {requestHeaders = [(hContentType, "application/json")]}
                    resp <- httpLbs req manager
                    statusCode (responseStatus resp) @?= 200
                    responseBody resp @?=
                        "{\"0\":{\"issuer\":" <> LBS8.pack (show eurBank)
                        <> ",\"owner\":"<> LBS8.pack (show alice)
                        <> ",\"currency\":\"EUR\",\"amount\":100.0000000000,\"observers\":[]}}"
                    -- Note (MK) You might be tempted to suggest using
                    -- create_group and interruptProcessGroupOf
                    -- or alternatively use_process_jobs here.
                    -- However, that is a trap. It will block forever
                    -- trying to terminate the process on Windows. I have absolutely
                    -- no idea why that is the case and I stopped trying
                    -- to figure out.
                    -- Luckily, it doesn’t seem like maven actually creates
                    -- child processes or at least none that
                    -- block us from cleaning up the SDK installation and
                    -- Bazel will tear down everything at the end anyway.
                    terminateProcess mavenPh
        subtest "daml codegen java with DAML_PROJECT" $ do
            withTempDir $ \dir -> do
                callCommandSilentIn dir $ unwords ["daml", "new", dir </> "quickstart", "--template=quickstart-java"]
                let projEnv = [("DAML_PROJECT", dir </> "quickstart")]
                callCommandSilentWithEnvIn dir projEnv "daml build"
                callCommandSilentWithEnvIn dir projEnv "daml codegen java"
                pure ()
  where
    mvnRepoFlag = "-Dmaven.repo.local=" <> mvnDir

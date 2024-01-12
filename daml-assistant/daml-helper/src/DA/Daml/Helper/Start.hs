-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Start
    ( runStart

    , withJar
    , withSandbox
    , StartOptions(..)
    , SandboxPort(..)
    , SandboxPortSpec(..)
    , toSandboxPortSpec
    , JsonApiPort(..)
    , SandboxCantonPortSpec(..)
    ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad
import Control.Monad.Extra hiding (fromMaybeM)
import Data.Maybe
import DA.PortFile
import qualified Data.Text as T
import Network.Socket.Extended (getFreePort)
import System.Console.ANSI
import System.FilePath
import System.Process.Typed
import System.IO.Extra
import System.Info.Extra

import DA.Daml.Helper.Codegen
import DA.Daml.Helper.Ledger
import DA.Daml.Helper.Util
import DA.Daml.Project.Config
import DA.Daml.Project.Consts

data SandboxPortSpec = FreePort | SpecifiedPort SandboxPort

toSandboxPortSpec :: Int -> Maybe SandboxPortSpec
toSandboxPortSpec n
  | n < 0 = Nothing
  | n == 0 = Just FreePort
  | otherwise = Just (SpecifiedPort (SandboxPort n))

newtype SandboxPort = SandboxPort { unSandboxPort :: Int }
newtype JsonApiPort = JsonApiPort { unJsonApiPort :: Int }

-- | Use SandboxPortSpec to determine a sandbox port number.
-- This is racy thanks to getFreePort, but there's no good alternative at the moment.
getPortForSandbox :: SandboxPortSpec -> Maybe SandboxPortSpec -> IO Int
getPortForSandbox defaultPortSpec portSpecM =
    case fromMaybe defaultPortSpec portSpecM of
        SpecifiedPort port -> pure (unSandboxPort port)
        FreePort -> fromIntegral <$> getFreePort

determineCantonOptions :: Maybe SandboxPortSpec -> SandboxCantonPortSpec -> FilePath -> Maybe JsonApiPort -> IO CantonOptions
determineCantonOptions ledgerApiSpec SandboxCantonPortSpec{..} portFile jsonApi = do
    cantonLedgerApi <- getPortForSandbox (SpecifiedPort (SandboxPort (ledger defaultSandboxPorts))) ledgerApiSpec
    cantonAdminApi <- getPortForSandbox (SpecifiedPort (SandboxPort (admin defaultSandboxPorts))) adminApiSpec
    cantonDomainPublicApi <- getPortForSandbox (SpecifiedPort (SandboxPort (domainPublic defaultSandboxPorts))) domainPublicApiSpec
    cantonDomainAdminApi <- getPortForSandbox (SpecifiedPort (SandboxPort (domainAdmin defaultSandboxPorts))) domainAdminApiSpec
    let cantonPortFileM = Just portFile -- TODO allow canton port file to be passed in from command line?
    let cantonStaticTime = StaticTime False
    let cantonHelp = False
    let cantonConfigFiles = []
    let cantonJsonApi = fmap unJsonApiPort jsonApi
    let cantonJsonApiPortFileM = Nothing
    pure CantonOptions {..}

withSandbox :: StartOptions -> FilePath -> [String] -> (Process () () () -> SandboxPort -> IO a) -> IO a
withSandbox StartOptions{..} darPath sandboxArgs kont =
    cantonSandbox
  where
    cantonSandbox = withTempDir $ \tempDir -> do
      let portFile = tempDir </> "sandbox-portfile"
      cantonOptions <- determineCantonOptions sandboxPortM sandboxPortSpec portFile jsonApiPortM
      withCantonSandbox cantonOptions sandboxArgs $ \ph -> do
        putStrLn "Waiting for canton sandbox to start."
        sandboxPort <- readPortFileWith decodeCantonSandboxPort (unsafeProcessHandle ph) maxRetries portFile
        runLedgerUploadDar (sandboxLedgerFlags sandboxPort) (Just darPath)
        kont ph (SandboxPort sandboxPort)

waitForJsonApi :: Process () () () -> JsonApiPort -> IO ()
waitForJsonApi sandboxPh (JsonApiPort jsonApiPort) = do
        putStrLn "Waiting for JSON API to start: "
        waitForHttpServer 240 (unsafeProcessHandle sandboxPh) (putStr "." *> threadDelay 500000)
            ("http://localhost:" <> show jsonApiPort <> "/readyz") []

withOptsFromProjectConfig :: T.Text -> [String] -> ProjectConfig -> IO [String]
withOptsFromProjectConfig fieldName cliOpts projectConfig = do
    optsYaml :: [String] <-
        fmap (fromMaybe []) $
        requiredE ("Failed to parse " <> fieldName) $
        queryProjectConfig [fieldName] projectConfig
    pure (optsYaml ++ cliOpts)

data StartOptions = StartOptions
    { sandboxPortM :: Maybe SandboxPortSpec
    , jsonApiPortM :: Maybe JsonApiPort
    , onStartM :: Maybe String
    , shouldWaitForSignal :: Bool
    , sandboxOptions :: [String]
    , scriptOptions :: [String]
    , sandboxPortSpec :: !SandboxCantonPortSpec
    }

data SandboxCantonPortSpec = SandboxCantonPortSpec
  { adminApiSpec :: !(Maybe SandboxPortSpec)
  , domainPublicApiSpec :: !(Maybe SandboxPortSpec)
  , domainAdminApiSpec :: !(Maybe SandboxPortSpec)
  }

runStart :: StartOptions -> IO ()
runStart startOptions@StartOptions{..} =
  withProjectRoot Nothing (ProjectCheck "daml start" True) $ \_ _ -> do
    projectConfig <- getProjectConfig Nothing
    darPath <- getDarPath
    mbInitScript :: Maybe String <-
        requiredE "Failed to parse init-script" $
        queryProjectConfig ["init-script"] projectConfig
    sandboxOpts <- withOptsFromProjectConfig "sandbox-options" sandboxOptions projectConfig
    scriptOpts <- withOptsFromProjectConfig "script-options" scriptOptions projectConfig
    doBuild
    doCodegen projectConfig
    withSandbox startOptions darPath sandboxOpts $ \sandboxPh sandboxPort -> do
        let doRunInitScript =
              whenJust mbInitScript $ \initScript -> do
                  putStrLn "Running the initialization script."
                  procScript <- toAssistantCommand $
                      [ "script"
                      , "--dar"
                      , darPath
                      , "--script-name"
                      , initScript
                      , if any (`elem` ["-s", "--static-time"]) sandboxOpts
                          then "--static-time"
                          else "--wall-clock-time"
                      , "--ledger-host"
                      , "localhost"
                      , "--ledger-port"
                      , show (unSandboxPort sandboxPort)
                      ] ++ scriptOpts
                  runProcess_ procScript
        doRunInitScript
        listenForKeyPress projectConfig darPath sandboxPort doRunInitScript
        do
            whenJust onStartM $ \onStart -> runProcess_ (shell onStart)
            whenJust jsonApiPortM $ \jsonApiPort -> waitForJsonApi sandboxPh jsonApiPort
            when shouldWaitForSignal $
              void $ waitAnyCancel =<< mapM (async . waitExitCode) [sandboxPh]

    where
        doCodegen projectConfig =
          forM_ [minBound :: Lang .. maxBound :: Lang] $ \lang -> do
            mbOutputPath :: Maybe String <-
              requiredE ("Failed to parse codegen entry for " <> showLang lang) $
              queryProjectConfig
                ["codegen", showLang lang, "output-directory"]
                projectConfig
            whenJust mbOutputPath $ \_outputPath -> do
              runCodegen lang []
        doReset (SandboxPort sandboxPort) =
          runLedgerReset (sandboxLedgerFlags sandboxPort)
        doUploadDar darPath (SandboxPort sandboxPort) =
          runLedgerUploadDar (sandboxLedgerFlags sandboxPort) (Just darPath)
        listenForKeyPress projectConfig darPath sandboxPort runInitScript = do
          hSetBuffering stdin NoBuffering
          void $
            forkIO $
             do
              threadDelay 20000000
              -- give sandbox 20 secs to startup before printing rebuild instructions
              forever $ do
                printRebuildInstructions
                c <- getChar
                when (c == 'r' || c == 'R') $ rebuild projectConfig darPath sandboxPort runInitScript
                threadDelay 1000000
        rebuild :: ProjectConfig -> FilePath -> SandboxPort -> IO () -> IO ()
        rebuild projectConfig darPath sandboxPort doRunInitScript = do
          putStrLn "Re-building and uploading package ..."
          doReset sandboxPort
          doBuild
          doCodegen projectConfig
          doUploadDar darPath sandboxPort
          doRunInitScript
          setSGR [SetColor Foreground Dull Green]
          putStrLn "Rebuild complete."
          setSGR [Reset]
        printRebuildInstructions = do
          setSGR [SetColor Foreground Vivid Yellow]
          putStrLn reloadInstructions
          setSGR [Reset]
          hFlush stdout
        reloadInstructions
          | isWindows = "\nPress 'r' + 'Enter' to re-build and upload the package to the sandbox.\nPress 'Ctrl-C' to quit."
          | otherwise = "\nPress 'r' to re-build and upload the package to the sandbox.\nPress 'Ctrl-C' to quit."

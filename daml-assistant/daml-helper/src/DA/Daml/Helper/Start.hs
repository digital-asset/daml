-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Start
    ( runStart
    , runPlatformJar

    , withJar
    , withSandbox
    , withNavigator

    , NavigatorPort(..)
    , SandboxPort(..)
    , SandboxPortSpec(..)
    , toSandboxPortSpec
    , JsonApiPort(..)
    , JsonApiConfig(..)
    , OpenBrowser(..)
    , StartNavigator(..)
    , WaitForSignal(..)
    , SandboxOptions(..)
    , NavigatorOptions(..)
    , JsonApiOptions(..)
    , ScriptOptions(..)
    , SandboxClassic(..)
    ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Control.Monad.Extra hiding (fromMaybeM)
import qualified Data.HashMap.Strict as HashMap
import Data.Maybe
import qualified Data.Map.Strict as Map
import DA.PortFile
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Network.HTTP.Simple as HTTP
import System.Console.ANSI
import System.Environment (getEnvironment, getEnv, lookupEnv)
import System.FilePath
import System.Process.Typed
import System.IO.Extra
import System.Info.Extra
import Web.Browser
import qualified Web.JWT as JWT
import Data.Aeson

import DA.Daml.Helper.Codegen
import DA.Daml.Helper.Ledger
import DA.Daml.Helper.Util
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

-- [Note] The `platform-version` field:
--
-- Platform commands (at this point `daml sandbox`, `daml sandbox-classic` and `daml json-api`)
-- are handled as follows:
--
-- 1. The assistant invokes `daml-helper` as usual. The assistant is not aware of the
--    `platform-version` field or what is and what is not a platform command.
-- 2. `daml-helper` reads the `DAML_PLATFORM_VERSION` env var falling back to
--    the `platform-version` field from `daml.yaml`.
-- 3.1. If the platform version is equal to the SDK version (from `DAML_SDK_VERSION`),
--      `daml-helper` invokes the underlying tools (sandbox, JSON API, …) directly
--      and we are finished.
-- 3.2. If the platform version is different from the SDK version, `daml-helper`
--      invokes the underlying tool via the assistant, setting both `DAML_SDK_VERSION`
--      and `DAML_PLATFORM_VERSION` to the platform version.
-- 4. The assistant will now invoke `daml-helper` from the platform version SDK.
--    At this point, we are guaranteed to fall into 3.1 and daml-helper invokes the
--    tool directly.
--
-- Note that this supports `platform-version`s for SDKs that are not
-- aware of `platform-version`. In that case, `daml-helper` only has
-- the codepath for invoking the underlying tool so we are also guaranteed
-- to go to step 3.1.

data SandboxPortSpec = FreePort | SpecifiedPort SandboxPort

toSandboxPortSpec :: Int -> Maybe SandboxPortSpec
toSandboxPortSpec n
  | n < 0 = Nothing
  | n  == 0 = Just FreePort
  | otherwise = Just (SpecifiedPort (SandboxPort n))

fromSandboxPortSpec :: SandboxPortSpec -> Int
fromSandboxPortSpec FreePort = 0
fromSandboxPortSpec (SpecifiedPort (SandboxPort n)) = n

newtype SandboxPort = SandboxPort Int
newtype NavigatorPort = NavigatorPort Int
newtype JsonApiPort = JsonApiPort Int

navigatorPortNavigatorArgs :: NavigatorPort -> [String]
navigatorPortNavigatorArgs (NavigatorPort p) = ["--port", show p]

navigatorURL :: NavigatorPort -> String
navigatorURL (NavigatorPort p) = "http://localhost:" <> show p

withSandbox :: SandboxClassic -> SandboxPortSpec -> [String] -> (Process () () () -> SandboxPort -> IO a) -> IO a
withSandbox (SandboxClassic classic) portSpec extraArgs a = withTempFile $ \portFile -> do
    let sandbox = if classic then "sandbox-classic" else "sandbox"
    let args = [ sandbox
               , "--port", show (fromSandboxPortSpec portSpec)
               , "--port-file", portFile
               ] ++ extraArgs
    withPlatformJar args "sandbox-logback.xml" $ \ph -> do
        putStrLn "Waiting for sandbox to start: "
        port <- readPortFile maxRetries portFile
        a ph (SandboxPort port)

withNavigator :: SandboxPort -> NavigatorPort -> [String] -> (Process () () () -> IO a) -> IO a
withNavigator (SandboxPort sandboxPort) navigatorPort args a = do
    let navigatorArgs = concat
            [ ["server", "localhost", show sandboxPort]
            , navigatorPortNavigatorArgs navigatorPort
            , args
            ]
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "navigator-logback.xml")
    withJar damlSdkJar [logbackArg] ("navigator":navigatorArgs) $ \ph -> do
        putStrLn "Waiting for navigator to start: "
        -- TODO We need to figure out a sane timeout for this step.
        waitForHttpServer (putStr "." *> threadDelay 500000) (navigatorURL navigatorPort) []
        a ph

withJsonApi :: SandboxPort -> JsonApiPort -> [String] -> (Process () () () -> IO a) -> IO a
withJsonApi (SandboxPort sandboxPort) (JsonApiPort jsonApiPort) extraArgs a = do
    let args =
            [ "json-api"
            , "--ledger-host", "localhost"
            , "--ledger-port", show sandboxPort
            , "--http-port", show jsonApiPort
            , "--allow-insecure-tokens"
            ] ++ extraArgs
    withPlatformJar args "json-api-logback.xml" $ \ph -> do
        putStrLn "Waiting for JSON API to start: "
        -- The secret doesn’t matter here
        let token = JWT.encodeSigned (JWT.HMACSecret "secret") mempty mempty
                { JWT.unregisteredClaims = JWT.ClaimsMap $
                      Map.fromList [("https://daml.com/ledger-api", Object $ HashMap.fromList [("actAs", toJSON ["Alice" :: T.Text]), ("ledgerId", "MyLedger"), ("applicationId", "foobar")])]
                }
        -- For now, we have a dummy authorization header here to wait for startup since we cannot get a 200
        -- response otherwise. We probably want to add some method to detect successful startup without
        -- any authorization
        let headers =
                [ ("Authorization", "Bearer " <> T.encodeUtf8 token)
                ] :: HTTP.RequestHeaders
        waitForHttpServer (putStr "." *> threadDelay 500000) ("http://localhost:" <> show jsonApiPort <> "/v1/query") headers
        a ph

-- | Whether `daml start` should open a browser automatically.
newtype OpenBrowser = OpenBrowser Bool

-- | Whether `daml start` should start the navigator automatically.
newtype StartNavigator = StartNavigator Bool

data JsonApiConfig = JsonApiConfig
  { mbJsonApiPort :: Maybe JsonApiPort -- If Nothing, don’t start the JSON API
  }

-- | Whether `daml start` should wait for Ctrl+C or interrupt after starting servers.
newtype WaitForSignal = WaitForSignal Bool

newtype SandboxOptions = SandboxOptions [String]
newtype NavigatorOptions = NavigatorOptions [String]
newtype JsonApiOptions = JsonApiOptions [String]
newtype ScriptOptions = ScriptOptions [String]
newtype SandboxClassic = SandboxClassic { unSandboxClassic :: Bool }

withOptsFromProjectConfig :: T.Text -> [String] -> ProjectConfig -> IO [String]
withOptsFromProjectConfig fieldName cliOpts projectConfig = do
    optsYaml :: [String] <-
        fmap (fromMaybe []) $
        requiredE ("Failed to parse " <> fieldName) $
        queryProjectConfig [fieldName] projectConfig
    pure (optsYaml ++ cliOpts)


runStart
    :: Maybe SandboxPortSpec
    -> Maybe StartNavigator
    -> NavigatorPort
    -> JsonApiConfig
    -> OpenBrowser
    -> Maybe String
    -> WaitForSignal
    -> SandboxOptions
    -> NavigatorOptions
    -> JsonApiOptions
    -> ScriptOptions
    -> SandboxClassic
    -> IO ()
runStart
  sandboxPortM
  mbStartNavigator
  navigatorPort
  (JsonApiConfig mbJsonApiPort)
  (OpenBrowser shouldOpenBrowser)
  onStartM
  (WaitForSignal shouldWaitForSignal)
  (SandboxOptions sandboxOpts)
  (NavigatorOptions navigatorOpts)
  (JsonApiOptions jsonApiOpts)
  (ScriptOptions scriptOpts)
  sandboxClassic
  = withProjectRoot Nothing (ProjectCheck "daml start" True) $ \_ _ -> do
    let sandboxPort = fromMaybe defaultSandboxPort sandboxPortM
    projectConfig <- getProjectConfig
    darPath <- getDarPath
    mbScenario :: Maybe String <-
        requiredE "Failed to parse scenario" $
        queryProjectConfig ["scenario"] projectConfig
    mbInitScript :: Maybe String <-
        requiredE "Failed to parse init-script" $
        queryProjectConfig ["init-script"] projectConfig
    shouldStartNavigator :: Bool <- case mbStartNavigator of
        -- If an option is passed explicitly, we use it, otherwise we read daml.yaml.
        Nothing ->
            fmap (fromMaybe True) $
            requiredE "Failed to parse start-navigator" $
            queryProjectConfig ["start-navigator"] projectConfig
        Just (StartNavigator explicit) -> pure explicit
    sandboxOpts <- withOptsFromProjectConfig "sandbox-options" sandboxOpts projectConfig
    navigatorOpts <- withOptsFromProjectConfig "navigator-options" navigatorOpts projectConfig
    jsonApiOpts <- withOptsFromProjectConfig "json-api-options" jsonApiOpts projectConfig
    scriptOpts <- withOptsFromProjectConfig "script-options" scriptOpts projectConfig
    doBuild
    doCodegen projectConfig
    listenForKeyPress projectConfig darPath
    let scenarioArgs = maybe [] (\scenario -> ["--scenario", scenario]) mbScenario
    withSandbox sandboxClassic sandboxPort (darPath : scenarioArgs ++ sandboxOpts) $ \sandboxPh sandboxPort -> do
        withNavigator' shouldStartNavigator sandboxPh sandboxPort navigatorPort navigatorOpts $ \navigatorPh -> do
            whenJust mbInitScript $ \initScript -> do
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
                    , case sandboxPort of SandboxPort port -> show port
                    ] ++ scriptOpts
                runProcess_ procScript
            whenJust onStartM $ \onStart -> runProcess_ (shell onStart)
            when (shouldStartNavigator && shouldOpenBrowser) $
                void $ openBrowser (navigatorURL navigatorPort)
            withJsonApi' sandboxPh sandboxPort jsonApiOpts $ \jsonApiPh -> do
                when shouldWaitForSignal $
                  void $ waitAnyCancel =<< mapM (async . waitExitCode) [navigatorPh,sandboxPh,jsonApiPh]

    where
        defaultSandboxPort = SpecifiedPort (SandboxPort 6865)
        withNavigator' shouldStartNavigator sandboxPh =
            if shouldStartNavigator
                then withNavigator
                else (\_ _ _ f -> f sandboxPh)
        withJsonApi' sandboxPh sandboxPort args f =
            case mbJsonApiPort of
                Nothing -> f sandboxPh
                Just jsonApiPort -> withJsonApi sandboxPort jsonApiPort args f
        doCodegen projectConfig =
          forM_ [minBound :: Lang .. maxBound :: Lang] $ \lang -> do
            mbOutputPath :: Maybe String <-
              requiredE ("Failed to parse codegen entry for " <> showLang lang) $
              queryProjectConfig
                ["codegen", showLang lang, "output-directory"]
                projectConfig
            whenJust mbOutputPath $ \_outputPath -> do
              runCodegen lang []
        doUploadDar darPath =
          runLedgerUploadDar (defaultLedgerFlags Grpc) (Just darPath)
        listenForKeyPress projectConfig darPath = do
          hSetBuffering stdin NoBuffering
          void $
            forkIO $
             do
              threadDelay 20000000
              -- give sandbox 20 secs to startup before printing rebuild instructions
              forever $ do
                printRebuildInstructions
                c <- getChar
                when (c == 'r' || c == 'R') $ rebuild projectConfig darPath
                threadDelay 1000000

        rebuild projectConfig darPath = do
          putStrLn "re-building and uploading package"
          doBuild
          doCodegen projectConfig
          doUploadDar darPath
          putStrLn "rebuild complete"
        printRebuildInstructions = do
          setSGR [SetColor Foreground Vivid Red]
          putStrLn reloadInstructions
          setSGR [Reset]
          hFlush stdout
        reloadInstructions
          | isWindows = "\nPress 'r' + 'Enter' to re-build and upload the package to the sandbox.\nPress 'Ctrl-C' to quit."
          | otherwise = "\nPress 'r' to re-build and upload the package to the sandbox.\nPress 'Ctrl-C' to quit."

platformVersionEnvVar :: String
platformVersionEnvVar = "DAML_PLATFORM_VERSION"

-- | Returns the platform version determined as follows:
--
-- 1. If DAML_PLATFORM_VERSION is set return that.
-- 2. If DAML_PROJECT is set and non-empty and `daml.yaml`
--    has a `platform-version` field return that.
-- 3. If `DAML_SDK_VERSION` is set return that.
-- 4. Else we are invoked outside of the assistant and we throw an exception.
getPlatformVersion :: IO String
getPlatformVersion = do
  mbPlatformVersion <- lookupEnv platformVersionEnvVar
  case mbPlatformVersion of
    Just platformVersion -> pure platformVersion
    Nothing -> do
      mbProjPath <- getProjectPath
      case mbProjPath of
        Just projPath -> do
          project <- readProjectConfig (ProjectPath projPath)
          case queryProjectConfig ["platform-version"] project of
            Left err -> throwIO err
            Right (Just ver) -> pure ver
            Right Nothing -> getSdkVersion
        Nothing -> getSdkVersion

-- Convenience-wrapper around `withPlatformProcess` for commands that call the SDK
-- JAR `daml-sdk.jar`.
withPlatformJar
    :: [String]
    -- ^ Commands passed to the assistant and the platform JAR.
    -> FilePath
    -- ^ File name of the logback config.
    -> (Process () () () -> IO a)
    -> IO a
withPlatformJar args logbackConf f = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> logbackConf)
    withPlatformProcess args (withJar damlSdkJar [logbackArg] args) f

runPlatformJar :: [String] -> FilePath -> IO ()
runPlatformJar args logback = do
    withPlatformJar args logback (const $ pure ())

withPlatformProcess
    :: [String]
    -- ^ List of commands passed to the assistant to invoke the command.
    -> ((Process () () () -> IO a) -> IO a)
    -- ^ Function to invoke the command in the current SDK.
    -> (Process () () () -> IO a)
    -> IO a
withPlatformProcess args runInCurrentSdk f = do
    platformVersion <- getPlatformVersion
    sdkVersion <- getSdkVersion
    if platformVersion == sdkVersion
       then runInCurrentSdk f
       else do
        assistant <- getEnv damlAssistantEnvVar
        env <- extendEnv
            [ (platformVersionEnvVar, platformVersion)
            , (sdkVersionEnvVar, platformVersion)
            ]
        withProcessWait_
            ( setEnv env
            $ proc assistant args
            )
            f

extendEnv :: [(String, String)] -> IO [(String, String)]
extendEnv xs = do
    oldEnv <- getEnvironment
    -- DAML_SDK is version specific so we need to filter it.
    pure (xs ++ filter (\(k, _) -> k `notElem` (sdkPathEnvVar : map fst xs)) oldEnv)

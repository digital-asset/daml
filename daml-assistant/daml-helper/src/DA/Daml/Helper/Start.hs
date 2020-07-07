-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE MultiWayIf #-}
module DA.Daml.Helper.Start
    ( runStart

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
import Control.Monad
import Control.Monad.Extra hiding (fromMaybeM)
import qualified Data.HashMap.Strict as HashMap
import Data.Maybe
import qualified Data.Map.Strict as Map
import DA.PortFile
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Network.HTTP.Simple as HTTP
import System.FilePath
import System.Process.Typed
import System.IO.Extra
import Web.Browser
import qualified Web.JWT as JWT
import Data.Aeson

import DA.Daml.Helper.Util
import DA.Daml.Project.Config
import DA.Daml.Project.Consts

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
withSandbox (SandboxClassic classic) portSpec args a = withTempFile $ \portFile -> do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "sandbox-logback.xml")
    let sandbox = if classic then "sandbox-classic" else "sandbox"
    withJar damlSdkJar [logbackArg] ([sandbox, "--port", show (fromSandboxPortSpec portSpec), "--port-file", portFile] ++ args) $ \ph -> do
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
withJsonApi (SandboxPort sandboxPort) (JsonApiPort jsonApiPort) args a = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "json-api-logback.xml")
    let jsonApiArgs =
            ["--ledger-host", "localhost", "--ledger-port", show sandboxPort,
             "--http-port", show jsonApiPort, "--allow-insecure-tokens"] <> args
    withJar damlSdkJar [logbackArg] ("json-api":jsonApiArgs) $ \ph -> do
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
        navigatorPort = NavigatorPort 7500
        defaultSandboxPort = SpecifiedPort (SandboxPort 6865)
        withNavigator' shouldStartNavigator sandboxPh =
            if shouldStartNavigator
                then withNavigator
                else (\_ _ _ f -> f sandboxPh)
        withJsonApi' sandboxPh sandboxPort args f =
            case mbJsonApiPort of
                Nothing -> f sandboxPh
                Just jsonApiPort -> withJsonApi sandboxPort jsonApiPort args f

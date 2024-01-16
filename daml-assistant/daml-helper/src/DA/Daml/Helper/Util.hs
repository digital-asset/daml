-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Util
  ( DamlHelperError(..)
  , requiredE
  , findDamlProjectRoot
  , doBuild
  , getDarPath
  , getProjectLedgerHost
  , getProjectLedgerPort
  , getProjectLedgerAccessToken
  , getProjectParties
  , getProjectConfig
  , toAssistantCommand
  , withProcessWait_'
  , damlSdkJar
  , damlSdkJarFolder
  , withJar
  , runJar
  , runCantonSandbox
  , withCantonSandbox
  , withCantonPortFile
  , getLogbackArg
  , waitForHttpServer
  , tokenFor
  , StaticTime(..)
  , SandboxPorts(..)
  , defaultSandboxPorts
  , CantonOptions(..)
  , decodeCantonPort
  , decodeCantonSandboxPort
  , CantonReplApi(..)
  , CantonReplParticipant(..)
  , CantonReplDomain(..)
  , CantonReplOptions(..)
  , runCantonRepl
  ) where

import Control.Exception.Safe
import Control.Monad.Extra
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Key as Aeson.Key
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified Network.HTTP.Simple as HTTP
import qualified Network.HTTP.Types as HTTP
import System.Directory
import System.FilePath
import System.IO
import System.IO.Extra (withTempDir, withTempFile)
import System.Info.Extra
import System.Exit (exitFailure)
import System.Process (ProcessHandle, getProcessExitCode, showCommandForUser, terminateProcess)
import System.Process.Typed
import qualified Web.JWT as JWT
import qualified Data.Aeson as A
import qualified Data.Map as Map

import DA.PortFile
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import DA.Daml.Project.Util hiding (fromMaybeM)

findDamlProjectRoot :: FilePath -> IO (Maybe FilePath)
findDamlProjectRoot = findAscendantWithFile projectConfigName

findAscendantWithFile :: FilePath -> FilePath -> IO (Maybe FilePath)
findAscendantWithFile filename path =
    findM (\p -> doesFileExist (p </> filename)) (ascendants path)

-- See [Note cmd.exe and why everything is horrible]
toAssistantCommand :: [String] -> IO (ProcessConfig () () ())
toAssistantCommand args = do
    assistant <- getDamlAssistant
    pure $ if isWindows
        then shell $ "\"" <> showCommandForUser assistant args <> "\""
        else shell $ showCommandForUser assistant args

doBuild :: IO ()
doBuild = do
    procConfig <- toAssistantCommand ["build"]
    runProcess_ procConfig

getDarPath :: IO FilePath
getDarPath = do
    projectName <- getProjectName
    projectVersion <- getProjectVersion
    return $ ".daml" </> "dist" </> projectName <> "-" <> projectVersion <> ".dar"

getProjectName :: IO String
getProjectName = do
    projectConfig <- getProjectConfig Nothing
    requiredE "Failed to read project name from project config" $
        queryProjectConfigRequired ["name"] projectConfig

getProjectVersion :: IO String
getProjectVersion = do
    projectConfig <- getProjectConfig Nothing
    requiredE "Failed to read project version from project config" $
        queryProjectConfigRequired ["version"] projectConfig

getProjectParties :: IO [String]
getProjectParties = do
    projectConfig <- getProjectConfig Nothing
    fmap (fromMaybe []) $
        requiredE "Failed to read list of parties from project config" $
        queryProjectConfig ["parties"] projectConfig

getProjectLedgerPort :: IO Int
getProjectLedgerPort = do
    projectConfig <- getProjectConfig $ Just "--port"
    -- TODO: remove default; insist ledger-port is in the config ?!
    defaultingE "Failed to parse ledger.port" 6865 $
        queryProjectConfig ["ledger", "port"] projectConfig

getProjectLedgerHost :: IO String
getProjectLedgerHost = do
    projectConfig <- getProjectConfig $ Just "--host"
    defaultingE "Failed to parse ledger.host" "localhost" $
        queryProjectConfig ["ledger", "host"] projectConfig

getProjectLedgerAccessToken :: IO (Maybe FilePath)
getProjectLedgerAccessToken = do
    projectConfigFpM <- getProjectPath
    projectConfigM <- forM projectConfigFpM (readProjectConfig . ProjectPath)
    case projectConfigM of
        Nothing -> pure Nothing
        Just projectConfig ->
            defaultingE "Failed to parse ledger.access-token-file" Nothing $
            queryProjectConfig ["ledger", "access-token-file"] projectConfig

getProjectConfig :: Maybe T.Text -> IO ProjectConfig
getProjectConfig argM = do
    projectPath <-
        required
            (case argM of
              Nothing -> "Must be called from within a project."
              Just arg -> "This command needs to be either run from within a project \
                          \ or the argument " <> arg <> " needs to be specified.") =<<
        getProjectPath
    readProjectConfig (ProjectPath projectPath)

requiredE :: Exception e => T.Text -> Either e t -> IO t
requiredE msg = fromRightM (throwIO . DamlHelperError msg . Just . T.pack . displayException)

defaultingE :: Exception e => T.Text -> a -> Either e (Maybe a) -> IO a
defaultingE msg a e = fmap (fromMaybe a) $ requiredE msg e

required :: T.Text -> Maybe t -> IO t
required msg = maybe (throwIO $ DamlHelperError msg Nothing) pure

data DamlHelperError = DamlHelperError
    { errMessage :: T.Text
    , errInternal :: Maybe T.Text
    } deriving (Eq, Show)

instance Exception DamlHelperError where
    displayException DamlHelperError{..} =
        T.unpack . T.unlines . catMaybes $
            [ Just ("daml: " <> errMessage)
            , fmap ("  details: " <>) errInternal
            ]

runJar :: FilePath -> Maybe FilePath -> [String] -> IO ()
runJar jarPath mbLogbackPath remainingArgs = do
    mbLogbackArg <- traverse getLogbackArg mbLogbackPath
    withJar jarPath (toList mbLogbackArg) remainingArgs (const $ pure ())

getLogbackArg :: FilePath -> IO String
getLogbackArg relPath = do
    sdkPath <- getSdkPath
    let logbackPath = sdkPath </> relPath
    pure $ "-Dlogback.configurationFile=" <> logbackPath

-- | This is a version of `withProcessWait_` that will make sure that the process dies
-- on an exception. The problem with `withProcessWait_` is that it tries to kill
-- `waitForProcess` with an async exception which does not work on Windows
-- since everything on Windows is terrible.
-- Therefore, we kill the process with `terminateProcess` first which will unblock
-- `waitForProcess`. Note that it is crucial to use `terminateProcess` instead of
-- `stopProcess` since the latter falls into the same issue of trying to kill
-- things which cannot be killed on Windows.
withProcessWait_' :: ProcessConfig stdin stdout stderr -> (Process stdin stdout stderr -> IO a) -> IO a
withProcessWait_' config f = bracket
    (startProcess config)
    stopProcess
    (\p -> (f p <* checkExitCode p) `onException` terminateProcess (unsafeProcessHandle p))

-- The first set of arguments is passed before -jar, the other after the jar path.
withJar :: FilePath -> [String] -> [String] -> (Process () () () -> IO a) -> IO a
withJar jarPath jvmArgs jarArgs a = do
    sdkPath <- getSdkPath
    let absJarPath = sdkPath </> jarPath
    withProcessWait_' (proc "java" (jvmArgs ++ ["-jar", absJarPath] ++ jarArgs)) a `catchIO`
        (\e -> hPutStrLn stderr "Failed to start java. Make sure it is installed and in the PATH." *> throwIO e)

damlSdkJarFolder :: FilePath
damlSdkJarFolder = "daml-sdk"

damlSdkJar :: FilePath
damlSdkJar = damlSdkJarFolder </> "daml-sdk.jar"

-- | `waitForHttpServer numTries processHandle sleep url headers` tries to establish an HTTP connection on
-- the given URL with the given headers, in a given number of tries. Between each connection request
-- it checks that a certain process is still alive and calls `sleep`.
waitForHttpServer :: Int -> ProcessHandle -> IO () -> String -> HTTP.RequestHeaders -> IO ()
waitForHttpServer 0 _processHandle _sleep url _headers = do
    hPutStrLn stderr ("Failed to connect to HTTP server " <> url <> " in time.")
    exitFailure
waitForHttpServer numTries processHandle sleep url headers = do
    request <- HTTP.parseRequest $ "HEAD " <> url
    request <- pure (HTTP.setRequestHeaders headers request)
    r <- tryJust (\e -> guard (isIOException e || isHttpException e)) $ HTTP.httpNoBody request
    case r of
        Right resp | HTTP.statusCode (HTTP.getResponseStatus resp) == 200 -> pure ()
        Right resp -> do
            hPutStrLn stderr ("HTTP server " <> url <> " replied with status code "
                <> show (HTTP.statusCode (HTTP.getResponseStatus resp)) <> ".")
            exitFailure
        Left _ -> do
            sleep
            status <- getProcessExitCode processHandle
            case status of
                Nothing -> waitForHttpServer (numTries-1) processHandle sleep url headers
                Just exitCode -> do
                    hPutStrLn stderr ("Failed to connect to HTTP server " <> url
                        <> " before process exited with " <> show exitCode)
                    exitFailure
    where isIOException e = isJust (fromException e :: Maybe IOException)
          isHttpException e = isJust (fromException e :: Maybe HTTP.HttpException)

tokenFor :: [T.Text] -> T.Text -> T.Text -> T.Text
tokenFor parties ledgerId applicationId =
  JWT.encodeSigned
    (JWT.EncodeHMACSecret "secret")
    mempty
    mempty
      { JWT.unregisteredClaims =
          JWT.ClaimsMap $
          Map.fromList
            [ ( "https://daml.com/ledger-api"
              , A.Object $
                KM.fromList
                  [ ("actAs", A.toJSON parties)
                  , ("ledgerId", A.String ledgerId)
                  , ("applicationId", A.String applicationId)
                  ])
            ]
      }

data SandboxPorts = SandboxPorts
  { ledger :: Int
  , admin :: Int
  , sequencerPublic :: Int
  , sequencerAdmin :: Int
  , mediatorAdmin :: Int
  }

defaultSandboxPorts :: SandboxPorts
defaultSandboxPorts = SandboxPorts 6865 6866 6867 6868 6869

runCantonSandbox :: CantonOptions -> [String] -> IO ()
runCantonSandbox options args = withCantonSandbox options args (const $ pure ())

withCantonSandbox ::
    CantonOptions -> [String] -> ((Process () () (), Int) -> IO a) -> IO a
withCantonSandbox options remainingArgs k = do
    sdkPath <- getSdkPath
    let cantonJar = sdkPath </> "canton" </> "canton.jar"
    withTempFile $ \config -> do
        withCantonPortFile options $ \options portFile -> do
        withTempFile $ \altPortFile -> do
            -- The port file is written by canton before the domain is bootstrapped. Some tests
            -- (and presumably users) rely on the port file being written as a signal that canton
            -- is ready to receive commands. In order not to break this clients, we pretend that
            -- the requested port file is "altPortFile". Then at the end of the bootstrap
            -- script, we copy altPortFile to portFile.
            let options' = options { cantonPortFileM = Just altPortFile }
            BSL.writeFile config (cantonConfig options')
            withTempFile $ \bootstrapScript -> do
                writeFile bootstrapScript (bootstrapScriptStr altPortFile portFile)
                let args
                        | cantonHelp options = ["--help"]
                        | otherwise = concatMap (\f -> ["-c", f]) (cantonConfigFiles options)
                withJar
                    cantonJar
                    []
                    ( "daemon"
                        : "-c"
                        : config
                        : "--bootstrap"
                        : bootstrapScript
                        : (args <> remainingArgs)
                    )
                    ( \ph -> do
                        sandboxPort <-
                            readPortFileWith
                                decodeCantonSandboxPort
                                (unsafeProcessHandle ph)
                                maxRetries
                                portFile
                        k (ph, sandboxPort)
                    )
  where
    bootstrapScriptStr altPortFile portFile =
        unlines
            [ "val staticDomainParameters = StaticDomainParameters.defaults(sequencer1.config.crypto)"
            , "val domainOwners = Seq(sequencer1, mediator1)"
            , "bootstrap.domain(\"mydomain\", Seq(sequencer1), Seq(mediator1), domainOwners, staticDomainParameters)"
            , "sandbox.domains.connect_local(sequencer1)"
            , "os.copy(os.Path(" <> show altPortFile <> "), os.Path(" <> show portFile <> "))"
            ]

-- | Obtain a path to use as canton portfile, and give updated options.
withCantonPortFile :: CantonOptions -> (CantonOptions -> FilePath -> IO a) -> IO a
withCantonPortFile options kont =
    case cantonPortFileM options of
        Nothing ->
            withTempDir $ \ tempDir -> do
                let portFile = tempDir </> "canton-portfile.json"
                kont options { cantonPortFileM = Just portFile } portFile
        Just portFile ->
            kont options portFile

newtype StaticTime = StaticTime Bool

data CantonOptions = CantonOptions
  { cantonLedgerApi :: Int
  , cantonAdminApi :: Int
  , cantonSequencerPublicApi :: Int
  , cantonSequencerAdminApi :: Int
  , cantonMediatorAdminApi :: Int
  , cantonPortFileM :: Maybe FilePath
  , cantonStaticTime :: StaticTime
  , cantonHelp :: Bool
  , cantonConfigFiles :: [FilePath]
  , cantonJsonApi :: Maybe Int
  , cantonJsonApiPortFileM :: Maybe FilePath
  }

cantonConfig :: CantonOptions -> BSL.ByteString
cantonConfig CantonOptions{..} =
    Aeson.encode $ Aeson.object
        [ "canton" Aeson..= Aeson.object
            [ "parameters" Aeson..= Aeson.object ( concat
                [ [ "ports-file" Aeson..= portFile | Just portFile <- [cantonPortFileM] ]
                , [ "clock" Aeson..= Aeson.object
                        [ "type" Aeson..= ("sim-clock" :: T.Text) ]
                  | StaticTime True <- [cantonStaticTime] ]
                ] )
            , "participants-x" Aeson..= Aeson.object
                [ "sandbox" Aeson..= Aeson.object
                    (
                     [ storage
                     , "admin-api" Aeson..= port cantonAdminApi
                     , "ledger-api" Aeson..= Aeson.object
                         [ "port" Aeson..= cantonLedgerApi
                         , "user-management-service" Aeson..= Aeson.object [ "enabled" Aeson..= True ]
                         -- Can be dropped once user mgmt is enabled by default
                         ]

                     ] <>
                     [ "testing-time" Aeson..= Aeson.object [ "type" Aeson..= ("monotonic-time" :: T.Text) ]
                     | StaticTime True <- [cantonStaticTime]
                     ] <>
                     [ "http-ledger-api-experimental" Aeson..= Aeson.object
                             [ "allow-insecure-tokens" Aeson..= True
                             , "server" Aeson..= Aeson.object ( concat
                               [ [ "port" Aeson..= port | Just port <- [cantonJsonApi] ]
                               , [ "port-file" Aeson..= portFile | Just portFile <- [cantonJsonApiPortFileM] ]
                               ])
                             ]
                     ]
                    )
                ]
            , "sequencers-x" Aeson..= Aeson.object
                [ "sequencer1" Aeson..= Aeson.object
                    [ "sequencer" Aeson..= Aeson.object
                        [ "config" Aeson..= Aeson.object [ storage ]
                        , "type" Aeson..= ("community-reference" :: T.Text)
                        ]
                    , storage
                    , "public-api" Aeson..= port cantonSequencerPublicApi
                    , "admin-api" Aeson..= port cantonSequencerAdminApi
                    ]
                ]
            , "mediators-x" Aeson..= Aeson.object
                [ "mediator1" Aeson..= Aeson.object
                     [ "admin-api" Aeson..= port cantonMediatorAdminApi
                     ]
                ]
            ]
        ]
  where
    port p = Aeson.object [ "port" Aeson..= p ]
    storage = "storage" Aeson..= Aeson.object [ "type" Aeson..= ("memory" :: T.Text) ]

decodeCantonPort :: String -> String -> Maybe Int
decodeCantonPort participantName json = do
    participants :: Map.Map String (Map.Map String Int) <- Aeson.decode (BSL8.pack json)
    ports <- Map.lookup participantName participants
    Map.lookup "ledgerApi" ports

decodeCantonSandboxPort :: String -> Maybe Int
decodeCantonSandboxPort = decodeCantonPort "sandbox"

data CantonReplApi = CantonReplApi
    { craHost :: String
    , craPort :: Int
    }

data CantonReplParticipant = CantonReplParticipant
    { crpName :: String
    , crpLedgerApi :: Maybe CantonReplApi
    , crpAdminApi :: Maybe CantonReplApi
    }

data CantonReplDomain = CantonReplDomain
    { crdName :: String
    , crdPublicApi :: Maybe CantonReplApi
    , crdAdminApi :: Maybe CantonReplApi
    }

data CantonReplOptions = CantonReplOptions
    { croParticipants :: [CantonReplParticipant]
    , croDomains :: [CantonReplDomain]
    }

cantonReplConfig :: CantonReplOptions -> BSL.ByteString
cantonReplConfig CantonReplOptions{..} =
    Aeson.encode $ Aeson.object
        [ "canton" Aeson..= Aeson.object
            [ "remote-participants" Aeson..= Aeson.object
                [ Aeson.Key.fromString crpName Aeson..= Aeson.object
                    (api "ledger-api" crpLedgerApi <> api "admin-api" crpAdminApi)
                | CantonReplParticipant {..} <- croParticipants
                ]
            , "remote-domains" Aeson..= Aeson.object
                [ Aeson.Key.fromString crdName Aeson..= Aeson.object
                    (api "public-api" crdPublicApi <> api "admin-api" crdAdminApi)
                | CantonReplDomain {..} <- croDomains
                ]
            ]
        ]
  where
    api name = \case
        Nothing -> []
        Just CantonReplApi{..} ->
            [ name Aeson..= Aeson.object
                [ "address" Aeson..= craHost
                , "port" Aeson..= craPort ] ]

runCantonRepl :: CantonReplOptions -> [String] -> IO ()
runCantonRepl options remainingArgs = do
    sdkPath <- getSdkPath
    let cantonJar = sdkPath </> "canton" </> "canton.jar"
    withTempFile $ \config -> do
        BSL.writeFile config (cantonReplConfig options)
        runJar cantonJar Nothing ("-c" : config : remainingArgs)

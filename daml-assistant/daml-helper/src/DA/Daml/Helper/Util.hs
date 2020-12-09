-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Util
  ( DamlHelperError(..)
  , requiredE
  , findDamlProjectRoot
  , doBuild
  , getDarPath
  , getProjectLedgerHost
  , getProjectLedgerPort
  , getProjectParties
  , getProjectConfig
  , toAssistantCommand
  , withProcessWait_'
  , damlSdkJar
  , damlSdkJarFolder
  , withJar
  , runJar
  , getLogbackArg
  , waitForConnectionOnPort
  , waitForHttpServer
  , tokenFor
  ) where

import Control.Exception.Safe
import Control.Monad.Extra
import Control.Monad.Loops (untilJust)
import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified Network.HTTP.Simple as HTTP
import qualified Network.HTTP.Types as HTTP
import Network.Socket
import System.Directory
import System.FilePath
import System.IO
import System.Info.Extra
import System.Process (showCommandForUser, terminateProcess)
import System.Process.Typed
import qualified Web.JWT as JWT
import qualified Data.Aeson as A
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map as Map
import qualified Data.Text.Lazy as TL

import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import DA.Daml.Project.Util hiding (fromMaybeM)
import qualified DA.Ledger as L

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
    projectConfig <- getProjectConfig
    requiredE "Failed to read project name from project config" $
        queryProjectConfigRequired ["name"] projectConfig

getProjectVersion :: IO String
getProjectVersion = do
    projectConfig <- getProjectConfig
    requiredE "Failed to read project version from project config" $
        queryProjectConfigRequired ["version"] projectConfig

getProjectParties :: IO [String]
getProjectParties = do
    projectConfig <- getProjectConfig
    requiredE "Failed to read list of parties from project config" $
        queryProjectConfigRequired ["parties"] projectConfig

getProjectLedgerPort :: IO Int
getProjectLedgerPort = do
    projectConfig <- getProjectConfig
    -- TODO: remove default; insist ledger-port is in the config ?!
    defaultingE "Failed to parse ledger.port" 6865 $
        queryProjectConfig ["ledger", "port"] projectConfig

getProjectLedgerHost :: IO String
getProjectLedgerHost = do
    projectConfig <- getProjectConfig
    defaultingE "Failed to parse ledger.host" "localhost" $
        queryProjectConfig ["ledger", "host"] projectConfig

getProjectConfig :: IO ProjectConfig
getProjectConfig = do
    projectPath <- required "Must be called from within a project" =<< getProjectPath
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

-- | `waitForConnectionOnPort sleep port` keeps trying to establish a TCP connection on the given port.
-- Between each connection request it calls `sleep`.
waitForConnectionOnPort :: IO () -> Int -> IO ()
waitForConnectionOnPort sleep port = do
    let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
    addr : _ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
    untilJust $ do
        r <- tryIO $ checkConnection addr
        case r of
            Left _ -> sleep *> pure Nothing
            Right _ -> pure $ Just ()
    where
        checkConnection addr = bracket
              (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
              close
              (\s -> connect s (addrAddress addr))

-- | `waitForHttpServer sleep url` keeps trying to establish an HTTP connection on the given URL.
-- Between each connection request it calls `sleep`.
waitForHttpServer :: IO () -> String -> HTTP.RequestHeaders -> IO ()
waitForHttpServer sleep url headers = do
    request <- HTTP.parseRequest $ "HEAD " <> url
    request <- pure (HTTP.setRequestHeaders headers request)
    untilJust $ do
        r <- tryJust (\e -> guard (isIOException e || isHttpException e)) $ HTTP.httpNoBody request
        case r of
            Right resp
                | HTTP.statusCode (HTTP.getResponseStatus resp) == 200 -> pure $ Just ()
            _ -> sleep *> pure Nothing
    where isIOException e = isJust (fromException e :: Maybe IOException)
          isHttpException e = isJust (fromException e :: Maybe HTTP.HttpException)

tokenFor :: [T.Text] -> T.Text -> T.Text
tokenFor parties ledgerId =
  JWT.encodeSigned
    (JWT.HMACSecret "secret")
    mempty
    mempty
      { JWT.unregisteredClaims =
          JWT.ClaimsMap $
          Map.fromList
            [ ( "https://daml.com/ledger-api"
              , A.Object $
                HashMap.fromList
                  [ ("actAs", A.toJSON parties)
                  , ("readAs", A.toJSON parties)
                  , ("ledgerId", A.String ledgerId)
                  ])
            ]
      }

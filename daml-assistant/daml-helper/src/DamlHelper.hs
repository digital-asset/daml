-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module DamlHelper
    ( runDamlStudio
    , runNew
    , runJar
    , runListTemplates
    , runSandbox
    , runStart

    , withJar
    , withSandbox
    , withNavigator

    , waitForConnectionOnPort
    , waitForHttpServer

    , NavigatorPort(..)
    , SandboxPort(..)
    ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception.Safe
import Control.Monad
import Control.Monad.Extra
import Control.Monad.Loops (untilJust)
import Data.Aeson
import Data.Aeson.Text
import Data.Maybe
import Data.List.Extra
import qualified Data.Text as T
import qualified Data.Text.Lazy as T (toStrict)
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Types as HTTP
import Network.Socket
import System.FilePath
import System.Directory.Extra
import System.Exit
import System.Process hiding (runCommand)
import System.IO
import System.IO.Error
import System.IO.Extra

import DAML.Project.Config
import DAML.Project.Consts
import DAML.Project.Types (ProjectPath(..))
import DAML.Project.Util

data DamlHelperError = DamlHelperError
    { errMessage :: T.Text
    , errInternal :: Maybe T.Text
    } deriving (Eq, Show)

instance Exception DamlHelperError where
    displayException DamlHelperError{..} =
        T.unpack . T.unlines . catMaybes $
            [ Just ("ERROR: " <> errMessage)
            , fmap ("  internal: " <>) errInternal
            ]

required :: T.Text -> Maybe t -> IO t
required msg = fromMaybeM (throwIO $ DamlHelperError msg Nothing)

requiredE :: Exception e => T.Text -> Either e t -> IO t
requiredE msg = fromRightM (throwIO . DamlHelperError msg . Just . T.pack . displayException)

runDamlStudio :: Bool -> [String] -> IO ()
runDamlStudio overwriteExtension remainingArguments = do
    sdkPath <- getSdkPath
    vscodeExtensionsDir <- fmap (</> ".vscode/extensions") getHomeDirectory
    let vscodeExtensionName = "da-vscode-daml-extension"
    let vscodeExtensionSrcDir = sdkPath </> "studio"
    let vscodeExtensionTargetDir = vscodeExtensionsDir </> vscodeExtensionName
    when overwriteExtension $ removePathForcibly vscodeExtensionTargetDir
    installExtension vscodeExtensionSrcDir vscodeExtensionTargetDir
    exitCode <- withCreateProcess (proc "code" ("-w" : remainingArguments)) $ \_ _ _ -> waitForProcess
    exitWith exitCode

runJar :: FilePath -> [String] -> IO ()
runJar jarPath remainingArguments = do
    exitCode <- withJar jarPath remainingArguments waitForProcess
    exitWith exitCode

withJar :: FilePath -> [String] -> (ProcessHandle -> IO a) -> IO a
withJar jarPath args a = do
    sdkPath <- getSdkPath
    let absJarPath = sdkPath </> jarPath
    withCreateProcess (proc "java" ("-jar" : absJarPath : args)) $ \_ _ _ -> a

getTemplatesFolder :: IO FilePath
getTemplatesFolder = fmap (</> "templates") getSdkPath

runNew :: FilePath -> String -> IO ()
runNew targetFolder templateName = do
    templatesFolder <- getTemplatesFolder
    let templateFolder = templatesFolder </> templateName
    unlessM (doesDirectoryExist templateFolder) $ do
        hPutStrLn stderr $ unlines
            [ "Template " <> show templateName <> " does not exist."
            , "Use `daml new --list` to see a list of available templates"
            ]
        exitFailure
    whenM (doesDirectoryExist targetFolder) $ do
        hPutStrLn stderr $ unlines
            [ "Directory " <> show targetFolder <> " already exists."
            , "Please specify a new directory for creating a project."
            ]
        exitFailure
    copyDirectory templateFolder targetFolder

    -- update daml.yaml
    let configPath = targetFolder </> projectConfigName
        configTemplatePath = configPath <.> "template"

    whenM (doesFileExist configTemplatePath) $ do
        configTemplate <- readFileUTF8 configTemplatePath
        sdkVersion <- getSdkVersion
        let projectName = takeFileName (dropTrailingPathSeparator targetFolder)
            config = replace "__VERSION__"  sdkVersion
                   . replace "__PROJECT_NAME__" projectName
                   $ configTemplate
        writeFileUTF8 configPath config
        removeFile configTemplatePath


runListTemplates :: IO ()
runListTemplates = do
    templatesFolder <- getTemplatesFolder
    templates <- listDirectory templatesFolder
    if null templates
       then putStrLn "No templates are available."
       else putStrLn $ unlines $
          "The following templates are available:" :
          map (\dir -> "  " <> takeFileName dir) templates


newtype SandboxPort = SandboxPort Int
newtype NavigatorPort = NavigatorPort Int

withSandbox :: SandboxPort -> [String] -> (ProcessHandle -> IO a) -> IO a
withSandbox (SandboxPort port) args a = do
    withJar sandboxPath (["--port", show port] ++ args) $ \ph -> do
        putStrLn "Waiting for sandbox to start: "
        -- TODO We need to figure out what a sane timeout for this step.
        waitForConnectionOnPort (putStr "." *> threadDelay 500000) port
        a ph

withNavigator :: SandboxPort -> NavigatorPort -> FilePath -> [String] -> (ProcessHandle-> IO a) -> IO a
withNavigator (SandboxPort sandboxPort) (NavigatorPort navigatorPort) config args a = do
    withJar navigatorPath (["server", "-c", config, "localhost", show sandboxPort, "--port", show navigatorPort] <> args) $ \ph -> do
        putStrLn "Waiting for navigator to start: "
        -- TODO We need to figure out a sane timeout for this step.
        waitForHttpServer (putStr "." *> threadDelay 500000) ("http://localhost:" <> show navigatorPort)
        a ph

runSandbox :: SandboxPort -> [String] -> IO ()
runSandbox port args = do
    exitCode <- withSandbox port args waitForProcess
    exitWith exitCode

runStart :: IO ()
runStart = withProjectRoot $ \_ -> do
    projectConfig <- getProjectConfig
    projectName :: String <-
        requiredE "Project must have a name" $
        queryProjectConfigRequired ["name"] projectConfig
    let darName = projectName <> ".dar"
    assistant <- getDamlAssistant
    callProcess assistant ["build", "-o", darName]
    withSandbox sandboxPort [darName] $ \sandboxPh -> do
        parties <- getProjectParties
        withTempDir $ \confDir -> do
            -- Navigator determines the file format based on the extension so we need a .json file.
            let navigatorConfPath = confDir </> "navigator-config.json"
            writeFileUTF8 navigatorConfPath (T.unpack $ navigatorConfig parties)
            withNavigator sandboxPort navigatorPort navigatorConfPath [] $ \navigatorPh ->
                void $ race (waitForProcess navigatorPh) (waitForProcess sandboxPh)
    where sandboxPort = SandboxPort 6865
          navigatorPort = NavigatorPort 7500


getProjectConfig :: IO ProjectConfig
getProjectConfig = do
    projectPath <- required "Must be called from within a project" =<< getProjectPath
    readProjectConfig (ProjectPath projectPath)

getProjectParties :: IO [T.Text]
getProjectParties = do
    projectConfig <- getProjectConfig
    requiredE "Project config does not have a list of parties" $ queryProjectConfigRequired ["parties"] projectConfig

navigatorConfig :: [T.Text] -> T.Text
navigatorConfig parties =
    T.toStrict $ encodeToLazyText $
       object ["users" .= object (map (\p -> p .= object [ "party" .= p ]) parties)]

copyDirectory :: FilePath -> FilePath -> IO ()
copyDirectory src target = do
    files <- listFilesRecursive src
    forM_ files $ \file -> do
        let baseName = makeRelative src file
        let targetFile = target </> baseName
        createDirectoryIfMissing True (takeDirectory targetFile)
        copyFile file targetFile
        p <- getPermissions targetFile
        setPermissions targetFile p { writable = True }

installExtension :: FilePath -> FilePath -> IO ()
installExtension src target =
    catchJust
        (guard . isAlreadyExistsError)
        (createDirectoryLink src target)
        (-- We might want to emit a warning if the extension is for a different SDK version
         -- but medium term it probably makes more sense to add the extension to the marketplace
         -- and make it backwards compatible
         const $ pure ())

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
waitForHttpServer :: IO () -> String -> IO ()
waitForHttpServer sleep url = do
    manager <- HTTP.newManager HTTP.defaultManagerSettings
    request <- HTTP.parseRequest $ "HEAD " <> url
    untilJust $ do
        r <- tryJust (\e -> guard (isIOException e || isHttpException e)) $ HTTP.httpNoBody request manager
        case r of
            Right resp
                | HTTP.statusCode (HTTP.responseStatus resp) == 200 -> pure $ Just ()
            _ -> sleep *> pure Nothing
    where isIOException e = isJust (fromException e :: Maybe IOException)
          isHttpException e = isJust (fromException e :: Maybe HTTP.HttpException)

sandboxPath :: FilePath
sandboxPath = "sandbox/sandbox.jar"

navigatorPath :: FilePath
navigatorPath = "navigator/navigator.jar"

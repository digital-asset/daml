-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module DamlHelper
    ( runDamlStudio
    , runInit
    , runNew
    , runMigrate
    , runJar
    , runListTemplates
    , runStart

    , withJar
    , withSandbox
    , withNavigator

    , waitForConnectionOnPort
    , waitForHttpServer

    , defaultProjectTemplate

    , NavigatorPort(..)
    , SandboxPort(..)
    , ReplaceExtension(..)
    , OpenBrowser(..)
    , DamlHelperError(..)
    ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception.Safe
import Control.Monad
import Control.Monad.Extra hiding (fromMaybeM)
import Control.Monad.Loops (untilJust)
import Data.Aeson
import Data.Aeson.Text
import Data.Maybe
import Data.List.Extra
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Lazy as T (toStrict)
import qualified Data.Yaml as Y
import qualified Data.Yaml.Pretty as Y
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Types as HTTP
import Network.Socket
import System.FilePath
import System.Directory.Extra
import System.Environment
import System.Exit
import System.Info.Extra
import System.Process hiding (runCommand)
import System.IO
import System.IO.Error
import System.IO.Extra
import Web.Browser

import DAML.Project.Config
import DAML.Project.Consts
import DAML.Project.Types
import DAML.Project.Util

import DamlHelper.Signals

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

required :: T.Text -> Maybe t -> IO t
required msg = fromMaybeM (throwIO $ DamlHelperError msg Nothing)

requiredE :: Exception e => T.Text -> Either e t -> IO t
requiredE msg = fromRightM (throwIO . DamlHelperError msg . Just . T.pack . displayException)

data ReplaceExtension
    = ReplaceExtNever
    -- ^ Never replace an existing extension.
    | ReplaceExtNewer
    -- ^ Replace the extension if the current extension is newer.
    | ReplaceExtAlways
    -- ^ Always replace the extension.

runDamlStudio :: ReplaceExtension -> [String] -> IO ()
runDamlStudio replaceExt remainingArguments = do
    sdkPath <- getSdkPath
    vscodeExtensionsDir <- fmap (</> ".vscode/extensions") getHomeDirectory
    let vscodeExtensionName = "da-vscode-daml-extension"
    let vscodeExtensionSrcDir = sdkPath </> "studio"
    let vscodeExtensionTargetDir = vscodeExtensionsDir </> vscodeExtensionName
    whenM (shouldReplaceExtension replaceExt vscodeExtensionTargetDir) $
        removePathForcibly vscodeExtensionTargetDir
    installExtension vscodeExtensionSrcDir vscodeExtensionTargetDir
    -- Note that it is important that we use `shell` rather than `proc` here as
    -- `proc` will look for `code.exe` in PATH which does not exist.
    projectPathM <- getProjectPath
    let codeCommand
            | isMac = "open -a \"Visual Studio Code\""
            | otherwise = "code"
        path = fromMaybe "." projectPathM
        command = unwords $ codeCommand : path : remainingArguments

    -- Strip DAML environment variables before calling vscode.
    originalEnv <- getEnvironment
    let strippedEnv = filter ((`notElem` damlEnvVars) . fst) originalEnv
        process = (shell command) { env = Just strippedEnv }

    exitCode <- withCreateProcess process $ \_ _ _ -> waitForProcess
    when (exitCode /= ExitSuccess) $
        hPutStrLn stderr $
          "Failed to launch Visual Studio Code." <>
          " See https://code.visualstudio.com/Download for installation instructions."
    exitWith exitCode

shouldReplaceExtension :: ReplaceExtension -> FilePath -> IO Bool
shouldReplaceExtension replaceExt dir =
    case replaceExt of
        ReplaceExtNever -> pure False
        ReplaceExtAlways -> pure True
        ReplaceExtNewer -> do
            let installedVersionFile = dir </> "VERSION"
            ifM (doesFileExist installedVersionFile)
              (do installedVersion <-
                      requiredE "Failed to parse version of VSCode extension" . parseVersion . T.strip . T.pack =<<
                      readFileUTF8 installedVersionFile
                  sdkVersion <- requiredE "Failed to parse SDK version" . parseVersion . T.pack =<< getSdkVersion
                  pure (sdkVersion > installedVersion))
              (pure True)
              -- ^ If the VERSION file does not exist, we must have installed an older version.

runJar :: FilePath -> [String] -> IO ()
runJar jarPath remainingArguments = do
    exitCode <- withJar jarPath remainingArguments waitForProcess
    exitWith exitCode

withJar :: FilePath -> [String] -> (ProcessHandle -> IO a) -> IO a
withJar jarPath args a = do
    sdkPath <- getSdkPath
    let absJarPath = sdkPath </> jarPath
    installSignalHandlers
    (withCreateProcess (proc "java" ("-jar" : absJarPath : args)) $ \_ _ _ -> a) `catchIO`
        (\e -> hPutStrLn stderr "Failed to start java. Make sure it is installed and in the PATH." *> throwIO e)

getTemplatesFolder :: IO FilePath
getTemplatesFolder = fmap (</> "templates") getSdkPath

-- | Initialize a daml project in the current or specified directory.
-- It will do the following (first that applies):
--
-- 1. If the target folder is actually a file, it will error out.
--
-- 2. If the target folder does not exist, it will error out and ask
-- the user if they meant to use daml new instead.
--
-- 3. If the target folder is a daml project root, it will do nothing
-- and let the user know the target is already a daml project.
--
-- 4. If the target folder is inside a daml project (transitively) but
-- is not the project root, it will do nothing and print out a warning.
--
-- 5. If the target folder is a da project root, it will create a
-- daml.yaml config file from the da.yaml config file, and let the
-- user know that it did that.
--
-- 6. If the target folder is inside a da project (transitively) but
-- is not the project root, it will error out with a message that lets
-- the user know what the project root is and suggests the user run
-- daml init on the project root.
--
-- 7. If none of the above, it will create a daml.yaml from scratch.
-- It will attempt to find a Main.daml source file in the project
-- directory tree, but if it does not it will use daml/Main.daml
-- as the default.
--
runInit :: Maybe FilePath -> IO ()
runInit targetFolderM = do
    currentDir <- getCurrentDirectory
    let targetFolder = fromMaybe currentDir targetFolderM
        targetFolderRel = makeRelative currentDir targetFolder
        projectConfigRel = normalise (targetFolderRel </> projectConfigName)
          -- ^ for display purposes

    -- cases 1 or 2
    unlessM (doesDirectoryExist targetFolder) $ do
        whenM (doesFileExist targetFolder) $ do
            hPutStr stderr $ unlines
                [ "ERROR: daml init target should be a directory, but is a file."
                , "    target = " <> targetFolderRel
                ]
            exitFailure

        hPutStr stderr $ unlines
            [ "ERROR: daml init target does not exist."
            , "    target = " <> targetFolderRel
            , ""
            , "To create a project directory use daml new instead:"
            , "    daml new " <> escapePath targetFolderRel
            ]
        exitFailure
    targetFolderAbs <- makeAbsolute targetFolder -- necessary to find project roots

    -- cases 3 or 4
    damlProjectRootM <- findDamlProjectRoot targetFolderAbs
    whenJust damlProjectRootM $ \projectRoot -> do
        let projectRootRel = makeRelative currentDir projectRoot
        hPutStrLn stderr $ "DAML project already initialized at " <> projectRootRel
        when (targetFolderAbs /= projectRoot) $ do
            hPutStr stderr $ unlines
                [ "WARNING: daml init target is not the DAML project root."
                , "    daml init target  = " <> targetFolder
                , "    DAML project root = " <> projectRootRel
                ]
        exitSuccess

    -- cases 5 or 6
    daProjectRootM <- findDaProjectRoot targetFolderAbs
    whenJust daProjectRootM $ \projectRoot -> do
        when (targetFolderAbs /= projectRoot) $ do
            let projectRootRel = makeRelative currentDir projectRoot
            hPutStr stderr $ unlines
                [ "ERROR: daml init target is not DA project root."
                , "    daml init target  = " <> targetFolder
                , "    DA project root   = " <> projectRootRel
                , ""
                , "To proceed with da.yaml migration, please use the project root:"
                , "    daml init " <> escapePath projectRootRel
                ]
            exitFailure

        let legacyConfigPath = projectRoot </> legacyConfigName
            legacyConfigRel = normalise (targetFolderRel </> legacyConfigName)
              -- ^ for display purposes

        daYaml <- requiredE ("Failed to parse " <> T.pack legacyConfigPath) =<<
            Y.decodeFileEither (projectRoot </> legacyConfigName)

        putStr $ unlines
            [ "Detected DA project."
            , "Migrating " <> legacyConfigRel <> " to " <> projectConfigRel
            ]

        let getField :: Y.FromJSON t => T.Text -> IO t
            getField name =
                required ("Failed to parse project." <> name <> " from " <> T.pack legacyConfigPath) $
                    flip Y.parseMaybe daYaml $ \y -> do
                        p <- y Y..: "project"
                        p Y..: name

        minimumSdkVersion <- getMinimumSdkVersion
        projSdkVersion :: SdkVersion <- getField "sdk-version"
        let newProjSdkVersion = max projSdkVersion minimumSdkVersion

        when (projSdkVersion < minimumSdkVersion) $ do
            putStr $ unlines
                [ ""
                , "WARNING: da.yaml SDK version " <> versionToString projSdkVersion <> " is too old for the new"
                , "assistant, so daml.yaml will use SDK version " <> versionToString newProjSdkVersion <> " instead."
                , ""
                ]

        projSource :: T.Text <- getField "source"
        projParties :: [T.Text] <- getField "parties"
        projName :: T.Text <- getField "name"
        projScenario :: T.Text <- getField "scenario"

        BS.writeFile (projectRoot </> projectConfigName) . Y.encodePretty yamlConfig $ Y.object
            [ ("sdk-version", Y.String (versionToText newProjSdkVersion))
            , ("name", Y.String projName)
            , ("source", Y.String projSource)
            , ("scenario", Y.String projScenario)
            , ("parties", Y.array (map Y.String projParties))
            , ("version", Y.String "1.0.0")
            , ("exposed-modules", Y.array [Y.String "Main"])
            , ("dependencies", Y.array [Y.String "daml-prim", Y.String "daml-stdlib"])
            ]

        putStrLn ("Done! Please verify " <> projectConfigRel)
        exitSuccess

    -- case 7
    putStrLn ("Generating " <> projectConfigRel)

    currentSdkVersion <- getSdkVersion

    projectFiles <- listFilesRecursive targetFolder
    let isMainDotDaml = (== "Main.daml") . takeFileName
        sourceM = listToMaybe (filter isMainDotDaml projectFiles)
        source = fromMaybe "daml/Main.daml" sourceM
        name = takeFileName (dropTrailingPathSeparator targetFolderAbs)

    BS.writeFile (targetFolder </> projectConfigName) . Y.encodePretty yamlConfig $ Y.object
        [ ("sdk-version", Y.String (T.pack currentSdkVersion))
        , ("name", Y.String (T.pack name))
        , ("source", Y.String (T.pack source))
        , ("scenario", Y.String "Main:mainScenario")
        , ("parties", Y.array [Y.String "Alice", Y.String "Bob"])
        , ("version", Y.String "1.0.0")
        , ("exposed-modules", Y.array [Y.String "Main"])
        , ("dependencies", Y.array [Y.String "daml-prim", Y.String "daml-stdlib"])
        ]

    putStr $ unlines
        [ "Initialized project " <> name
        , "Done! Please verify " <> projectConfigRel
        ]

    where

        getMinimumSdkVersion :: IO SdkVersion
        getMinimumSdkVersion =
            requiredE "BUG: Expected 0.12.15 to be valid SDK version" $
              parseVersion "0.12.15"

        fieldOrder :: [T.Text]
        fieldOrder =
            [ "sdk-version"
            , "name"
            , "version"
            , "source"
            , "scenario"
            , "parties"
            , "exposed-modules"
            , "dependencies"
            ]

        fieldNameCompare :: T.Text -> T.Text -> Ordering
        fieldNameCompare a b = compare (elemIndex a fieldOrder) (elemIndex b fieldOrder)

        yamlConfig :: Y.Config
        yamlConfig = Y.setConfCompare fieldNameCompare Y.defConfig

-- | Create a DAML project in a new directory, based on a project template packaged
-- with the SDK. Special care has been taken to avoid:
--
-- * Project name/template name confusion: i.e. when a user passes a
-- single argument, it should be the new project folder. But if the user
-- passes an existing template name instead, we ask the user to be more
-- explicit.
-- * Creation of a project in existing folder (suggest daml init instead).
-- * Creation of a project inside another project.
--
runNew :: FilePath -> Maybe String -> [String] -> IO ()
runNew targetFolder templateNameM pkgDeps = do
    templatesFolder <- getTemplatesFolder
    let templateName = fromMaybe defaultProjectTemplate templateNameM
        templateFolder = templatesFolder </> templateName
        projectName = takeFileName (dropTrailingPathSeparator targetFolder)

    -- Ensure template exists.
    unlessM (doesDirectoryExist templateFolder) $ do
        hPutStr stderr $ unlines
            [ "Template " <> show templateName <> " does not exist."
            , "Use `daml new --list` to see a list of available templates"
            ]
        exitFailure

    -- Ensure project directory does not already exist.
    whenM (doesDirectoryExist targetFolder) $ do
        hPutStr stderr $ unlines
            [ "Directory " <> show targetFolder <> " already exists."
            , "Please specify a new directory, or use 'daml init' instead:"
            , ""
            , "    daml init " <> escapePath targetFolder
            , ""
            ]
        exitFailure

    -- Ensure user is not confusing template name with project name.
    --
    -- We check projectName == targetFolder because if the user
    -- gave a targetFolder that isn't a straight up file name (it
    -- contains path separators), then it's likely that they did
    -- intend to pass a target folder and not a template name.
    when (isNothing templateNameM && projectName == targetFolder) $ do
        whenM (doesDirectoryExist (templatesFolder </> projectName)) $ do
            hPutStr stderr $ unlines
                [ "Template name " <> projectName <> " was given as project name."
                , "Please specify a project name separately, for example:"
                , ""
                , "    daml new myproject " <> projectName
                , ""
                ]
            exitFailure

    -- Ensure we are not creating a project inside another project.
    targetFolderAbs <- makeAbsolute targetFolder

    damlRootM <- findDamlProjectRoot targetFolderAbs
    whenJust damlRootM $ \damlRoot -> do
        hPutStr stderr $ unlines
            [ "Target directory is inside existing DAML project " <> show damlRoot
            , "Please specify a new directory outside an existing project."
            ]
        exitFailure

    daRootM <- findDaProjectRoot targetFolderAbs
    whenJust daRootM $ \daRoot -> do
        hPutStr stderr $ unlines
            [ "Target directory is inside existing DA project " <> show daRoot
            , "Please convert DA project to DAML using 'daml init':"
            , ""
            , "    daml init " <> escapePath daRoot
            , ""
            , "Or specify a new directory outside an existing project."
            ]
        exitFailure

    -- Copy the template over.
    copyDirectory templateFolder targetFolder
    files <- listFilesRecursive targetFolder
    mapM_ setWritable files

    -- Update daml.yaml
    let configPath = targetFolder </> projectConfigName
        configTemplatePath = configPath <.> "template"

    whenM (doesFileExist configTemplatePath) $ do
        configTemplate <- readFileUTF8 configTemplatePath
        sdkVersion <- getSdkVersion
        let config = replace "__VERSION__"  sdkVersion
                   . replace "__PROJECT_NAME__" projectName
                   . replace "__DEPENDENCIES__" (unlines ["  - " <> dep | dep <- pkgDeps])
                   $ configTemplate
        writeFileUTF8 configPath config
        removeFile configTemplatePath

    -- Done.
    putStrLn $
        "Created a new project in \"" <> targetFolder <>
        "\" based on the template \"" <> templateName <> "\"."

-- | Create a project containing code to migrate a running system between two given packages.
runMigrate :: FilePath -> FilePath -> FilePath -> IO ()
runMigrate targetFolder pkgPath1 pkgPath2
 = do
    pkgPath1Abs <- makeAbsolute pkgPath1
    pkgPath2Abs <- makeAbsolute pkgPath2
    -- Create a new project
    runNew targetFolder (Just "migrate") [pkgPath1Abs, pkgPath2Abs]

    -- Call damlc to create the upgrade source files.
    assistant <- getDamlAssistant
    callCommand
        (unwords $
         assistant :
         [ "damlc"
         , "migrate"
         , "--srcdir"
         , targetFolder </> "daml"
         , "upgrade-pkg"
         , pkgPath1
         , pkgPath2
         ])

defaultProjectTemplate :: String
defaultProjectTemplate = "skeleton"

legacyConfigName :: FilePath
legacyConfigName = "da.yaml"

findDamlProjectRoot :: FilePath -> IO (Maybe FilePath)
findDamlProjectRoot = findAscendantWithFile projectConfigName

findDaProjectRoot :: FilePath -> IO (Maybe FilePath)
findDaProjectRoot = findAscendantWithFile legacyConfigName

findAscendantWithFile :: FilePath -> FilePath -> IO (Maybe FilePath)
findAscendantWithFile filename path =
    findM (\p -> doesFileExist (p </> filename)) (ascendants path)

-- | Escape special characters in a filepath so they can be used as a shell
-- argument when displaying a suggested command to user. Do not use this to
-- invoke shell commands directly (there are libraries designed for that).
escapePath :: FilePath -> FilePath
escapePath p | isWindows = concat ["\"", p, "\""] -- Windows is a mess
escapePath p = p >>= \c ->
    if c `elem` (" \\\"\'$*{}#" :: String)
        then ['\\', c]
        else [c]

-- | Our SDK installation is read-only to prevent users from accidentally modifying it.
-- But when we copy from it in "daml new" we want the result to be writable.
setWritable :: FilePath -> IO ()
setWritable f = do
    p <- getPermissions f
    setPermissions f p { writable = True }

runListTemplates :: IO ()
runListTemplates = do
    templatesFolder <- getTemplatesFolder
    templates <- listDirectory templatesFolder
    if null templates
       then putStrLn "No templates are available."
       else putStrLn $ unlines $
          "The following templates are available:" :
          (map ("  " <>) . sort . map takeFileName) templates

newtype SandboxPort = SandboxPort Int
newtype NavigatorPort = NavigatorPort Int

navigatorPortNavigatorArgs :: NavigatorPort -> [String]
navigatorPortNavigatorArgs (NavigatorPort p) = ["--port", show p]

navigatorURL :: NavigatorPort -> String
navigatorURL (NavigatorPort p) = "http://localhost:" <> show p

withSandbox :: SandboxPort -> [String] -> (ProcessHandle -> IO a) -> IO a
withSandbox (SandboxPort port) args a = do
    withJar sandboxPath (["--port", show port] ++ args) $ \ph -> do
        putStrLn "Waiting for sandbox to start: "
        -- TODO We need to figure out what a sane timeout for this step.
        waitForConnectionOnPort (putStr "." *> threadDelay 500000) port
        a ph

withNavigator :: SandboxPort -> NavigatorPort -> FilePath -> [String] -> (ProcessHandle-> IO a) -> IO a
withNavigator (SandboxPort sandboxPort) navigatorPort config args a = do
    let navigatorArgs = concat
            [ ["server", "-c", config, "localhost", show sandboxPort]
            , navigatorPortNavigatorArgs navigatorPort
            , args
            ]
    withJar navigatorPath navigatorArgs $ \ph -> do
        putStrLn "Waiting for navigator to start: "
        -- TODO We need to figure out a sane timeout for this step.
        waitForHttpServer (putStr "." *> threadDelay 500000) (navigatorURL navigatorPort)
        a ph

-- | Whether `daml start` should open a browser automatically.
newtype OpenBrowser = OpenBrowser Bool

runStart :: OpenBrowser -> IO ()
runStart (OpenBrowser shouldOpenBrowser) = withProjectRoot Nothing (ProjectCheck "daml start" True) $ \_ _ -> do
    projectConfig <- getProjectConfig
    projectName :: String <-
        requiredE "Project must have a name" $
        queryProjectConfigRequired ["name"] projectConfig
    mbScenario :: Maybe String <-
        requiredE "Failed to parse scenario" $
        queryProjectConfig ["scenario"] projectConfig
    let darPath = "dist" </> projectName <> ".dar"
    assistant <- getDamlAssistant
    callCommand (unwords $ assistant : ["build", "-o", darPath])
    let scenarioArgs = maybe [] (\scenario -> ["--scenario", scenario]) mbScenario
    installSignalHandlers
    withSandbox sandboxPort (darPath : scenarioArgs) $ \sandboxPh -> do
        parties <- getProjectParties
        withTempDir $ \confDir -> do
            -- Navigator determines the file format based on the extension so we need a .json file.
            let navigatorConfPath = confDir </> "navigator-config.json"
            writeFileUTF8 navigatorConfPath (T.unpack $ navigatorConfig parties)
            withNavigator sandboxPort navigatorPort navigatorConfPath [] $ \navigatorPh -> do
                when shouldOpenBrowser $ void $ openBrowser (navigatorURL navigatorPort)
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

installExtension :: FilePath -> FilePath -> IO ()
installExtension src target = do
    -- Create .vscode/extensions if it does not already exist.
    createDirectoryIfMissing True (takeDirectory target)
    catchJust
        (guard . isAlreadyExistsError)
        install
        (const $ pure ())
        -- If we get an exception, we just keep the existing extension.
     where
         install
             | isWindows = do
                   -- We create the directory to throw an isAlreadyExistsError.
                   createDirectory target
                   copyDirectory src target
             | otherwise = createDirectoryLink src target

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

-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Run
    ( runDamlStudio
    , runInit
    , runNew
    , runMigrate
    , runJar
    , runDaml2ts
    , runListTemplates
    , runStart

    , LedgerFlags(..)
    , JsonFlag(..)
    , runDeploy
    , runLedgerAllocateParties
    , runLedgerListParties
    , runLedgerUploadDar
    , runLedgerNavigator

    , withJar
    , withSandbox
    , withNavigator

    , waitForConnectionOnPort
    , waitForHttpServer

    , defaultProjectTemplate

    , NavigatorPort(..)
    , SandboxPort(..)
    , JsonApiPort(..)
    , JsonApiConfig(..)
    , ReplaceExtension(..)
    , OpenBrowser(..)
    , StartNavigator(..)
    , WaitForSignal(..)
    , DamlHelperError(..)
    , SandboxOptions(..)
    , NavigatorOptions(..)
    , JsonApiOptions(..)
    ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception.Safe
import Control.Monad
import Control.Monad.Extra hiding (fromMaybeM)
import Control.Monad.Loops (untilJust)
import Data.Foldable
import Data.Maybe
import Data.List.Extra
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.UTF8 as UTF8
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Yaml as Y
import qualified Data.Yaml.Pretty as Y
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Types as HTTP
import Network.Socket
import System.FilePath
import qualified System.Directory as Dir
import System.Directory.Extra
import System.Environment hiding (setEnv)
import System.Exit
import System.Info.Extra
import System.Process (showCommandForUser)
import System.Process.Internals (translate)
import System.Process.Typed
import System.IO
import System.IO.Extra
import Web.Browser
import Data.Aeson
import Data.Aeson.Text

import DA.Daml.Helper.Ledger as Ledger
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import DA.Daml.Project.Util

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

defaultingE :: Exception e => T.Text -> a -> Either e (Maybe a) -> IO a
defaultingE msg a e = fmap (fromMaybe a) $ requiredE msg e

-- [Note cmd.exe and why everything is horrible]
--
-- cmd.exe has a terrible behavior where it strips the first and last
-- quote under certain conditions, quoting cmd.exe /?:
--
-- > 1.  If all of the following conditions are met, then quote characters
-- >     on the command line are preserved:
-- >
-- >     - no /S switch
-- >     - exactly two quote characters
-- >     - no special characters between the two quote characters,
-- >       where special is one of: &<>()@^|
-- >     - there are one or more whitespace characters between the
-- >       the two quote characters
-- >     - the string between the two quote characters is the name
-- >       of an executable file.
-- >
-- > 2.  Otherwise, old behavior is to see if the first character is
-- >     a quote character and if so, strip the leading character and
-- >     remove the last quote character on the command line, preserving
-- >     any text after the last quote character.
--
-- By adding a quote at the beginning and the end, we always fall into 2
-- and the line after stripping the first and last quote corresponds to the one we want.
--
-- To make things worse that does not seem to be sufficient sadly:
-- If we quote `"code"` it seems to result in cmd.exe no longer looking
-- for it in PATH. I was unable to find any documentation or other information
-- on this behavior.

-- See [Note cmd.exe and why everything is horrible]
toVsCodeCommand :: [String] -> ProcessConfig () () ()
toVsCodeCommand args
    | isWindows = shell $ "\"" <> unwords ("code" : map translate args) <> "\""
    | otherwise = shell $ showCommandForUser "code" args

-- See [Note cmd.exe and why everything is horrible]
toAssistantCommand :: [String] -> IO (ProcessConfig () () ())
toAssistantCommand args = do
    assistant <- getDamlAssistant
    pure $ if isWindows
        then shell $ "\"" <> showCommandForUser assistant args <> "\""
        else shell $ showCommandForUser assistant args

data ReplaceExtension
    = ReplaceExtNever
    -- ^ Never replace an existing extension.
    | ReplaceExtAlways
    -- ^ Always replace the extension.
    | ReplaceExtPublished
    -- ^ Replace with published extension (the default).

-- | Run VS Code command with arguments, returning the exit code, stdout & stderr.
runVsCodeCommand :: [String] -> IO (ExitCode, String, String)
runVsCodeCommand args = do
    originalEnv <- getEnvironment
    let strippedEnv = filter ((`notElem` damlEnvVars) . fst) originalEnv
            -- ^ Strip DAML environment variables before calling VSCode, to
            -- prevent setting DAML_SDK_VERSION too early. See issue #1666.
        commandEnv = addVsCodeToPath strippedEnv
            -- ^ Ensure "code" is in PATH before running command.
        command = toVsCodeCommand args
        process = setEnv commandEnv command
    (exit, out, err) <- readProcess process
    pure (exit, UTF8.toString out, UTF8.toString err)

-- | Add VSCode bin path to environment PATH. Only need to add it on Mac, as
-- VSCode is installed in PATH by default on the other platforms.
addVsCodeToPath :: [(String, String)] -> [(String,String)]
addVsCodeToPath env | isMac =
    let pathM = lookup "PATH" env
        newSearchPath = maybe "" (<> [searchPathSeparator]) pathM <>
            "/Applications/Visual Studio Code.app/Contents/Resources/app/bin"
    in ("PATH", newSearchPath) : filter ((/= "PATH") . fst) env
addVsCodeToPath env = env

-- | Directory where bundled extension gets installed.
getVsCodeExtensionsDir :: IO FilePath
getVsCodeExtensionsDir = fmap (</> ".vscode/extensions") getHomeDirectory

-- | Name of VS Code extension in the marketplace.
publishedExtensionName :: String
publishedExtensionName = "DigitalAssetHoldingsLLC.daml"

-- | Name of VS Code extension bundled with older SDKs (up to 0.13.12). Was
-- implemented as a symlink to extension files stored under ~/.daml.
oldBundledExtensionDirName :: String
oldBundledExtensionDirName = "da-vscode-daml-extension"

-- | Name of VS Code extension bundled with the SDK as a vsix.
bundledExtensionName :: String
bundledExtensionName = "DigitalAssetHoldingsLLC.daml-bundled"

-- | Status of installed VS Code extensions.
data InstalledExtensions = InstalledExtensions
    { oldBundled :: Maybe FilePath
        -- ^ bundled extension, if installed (0.13.12 and earlier)
    , bundledInstalled :: Bool
        -- ^ bundled extension, installed through vsix (0.13.13 and up)
    , publishedExtensionIsInstalled :: Bool
        -- ^ true if published extension is installed
    } deriving (Show, Eq)

-- | Get status of installed VS code extensions.
getInstalledExtensions :: IO InstalledExtensions
getInstalledExtensions = do
    oldBundled <- getOldExt
    extensions <- getExtensions
    let oldBundledIsInstalled = isJust oldBundled
        publishedExtensionIsInstalled = not oldBundledIsInstalled && publishedExtensionName `elem` extensions
        bundledInstalled = bundledExtensionName `elem` extensions
    pure InstalledExtensions {..}
    where getOldExt :: IO (Maybe FilePath)
          getOldExt = do
              extensionsDir <- getVsCodeExtensionsDir
              let oldBundledDir = extensionsDir </> oldBundledExtensionDirName
              exists <- Dir.doesPathExist oldBundledDir
              pure $ if exists then Just oldBundledDir else Nothing
          getExtensions :: IO [String]
          getExtensions = do
              (_exitCode, extensionsStr, _err) <- runVsCodeCommand ["--list-extensions"]
              pure $ lines extensionsStr

runDamlStudio :: ReplaceExtension -> [String] -> IO ()
runDamlStudio replaceExt remainingArguments = do
    sdkPath <- getSdkPath
    InstalledExtensions {..} <- getInstalledExtensions

    let bundledExtensionVsix = sdkPath </> "studio/daml-bundled.vsix"
        removeOldBundledExtension = whenJust oldBundled removePathForcibly
        uninstall name = do
            (exitCode, out, err) <- runVsCodeCommand ["--uninstall-extension", name]
            when (exitCode /= ExitSuccess) $ do
                hPutStrLn stderr . unlines $
                    [ "Failed to uninstall published version of DAML Studio."
                    , "Messages from VS Code:"
                    , "--- stdout ---"
                    , out
                    , "--- stderr ---"
                    , err
                    ]
                exitWith exitCode

        removeBundledExtension =
            when bundledInstalled $ uninstall bundledExtensionName

        removePublishedExtension =
            when publishedExtensionIsInstalled $ uninstall publishedExtensionName

        installBundledExtension' =
            installBundledExtension bundledExtensionVsix

        installPublishedExtension =
            when (not publishedExtensionIsInstalled) $ do
                (exitCode, _out, err) <- runVsCodeCommand
                    ["--install-extension", publishedExtensionName]
                when (exitCode /= ExitSuccess) $ do
                    hPutStr stderr . unlines $
                        [ err
                        , "Failed to install DAML Studio extension from marketplace."
                        , "Installing bundled DAML Studio extension instead."
                        ]
                    installBundledExtension'

    -- First, ensure extension is installed as requested.
    case replaceExt of
        ReplaceExtNever ->
            when (not bundledInstalled && isNothing oldBundled)
                installPublishedExtension

        ReplaceExtAlways -> do
            removePublishedExtension
            removeBundledExtension
            removeOldBundledExtension
            installBundledExtension'

        ReplaceExtPublished -> do
            removeBundledExtension
            removeOldBundledExtension
            installPublishedExtension

    -- Then, open visual studio code.
    projectPathM <- getProjectPath
    let path = fromMaybe "." projectPathM
    (exitCode, _out, err) <- runVsCodeCommand (path : remainingArguments)
    when (exitCode /= ExitSuccess) $ do
        hPutStrLn stderr . unlines $
            [ err
            , "Failed to launch DAML studio. Make sure Visual Studio Code is installed."
            , "See https://code.visualstudio.com/Download for installation instructions."
            ]
        exitWith exitCode

installBundledExtension :: FilePath -> IO ()
installBundledExtension pathToVsix = do
    (exitCode, _out, err) <- runVsCodeCommand ["--install-extension", pathToVsix]
    when (exitCode /= ExitSuccess) $ do
        hPutStr stderr . unlines $
           [ err
           , "Failed to install DAML Studio extension from SDK bundle."
           , "Please open an issue on GitHub with the above message."
           , "https://github.com/digital-asset/daml/issues/new?template=bug_report.md"
           ]

runJar :: FilePath -> Maybe FilePath -> [String] -> IO ()
runJar jarPath mbLogbackPath remainingArgs = do
    mbLogbackArg <- traverse getLogbackArg mbLogbackPath
    withJar jarPath (toList mbLogbackArg) remainingArgs (const $ pure ())

runDaml2ts :: [String] -> IO ()
runDaml2ts remainingArgs = do
  daml2ts <- fmap (</> "daml2ts" </> "daml2ts") getSdkPath
  withProcessWait_ (proc daml2ts remainingArgs) (const $ pure ()) `catchIO`
    (\e -> hPutStrLn stderr "Failed to invoke daml2ts." *> throwIO e)

getLogbackArg :: FilePath -> IO String
getLogbackArg relPath = do
    sdkPath <- getSdkPath
    let logbackPath = sdkPath </> relPath
    pure $ "-Dlogback.configurationFile=" <> logbackPath

-- The first set of arguments is passed before -jar, the other after the jar path.
withJar :: FilePath -> [String] -> [String] -> (Process () () () -> IO a) -> IO a
withJar jarPath jvmArgs jarArgs a = do
    sdkPath <- getSdkPath
    let absJarPath = sdkPath </> jarPath
    withProcessWait_ (proc "java" (jvmArgs ++ ["-jar", absJarPath] ++ jarArgs)) a `catchIO`
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
            , "    " <> showCommandForUser "daml" ["new", targetFolderRel]
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
                , "    " <> showCommandForUser "daml" ["init", projectRootRel]
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
    let targetFolderSep = addTrailingPathSeparator targetFolder
    let projectFilesRel = mapMaybe (stripPrefix targetFolderSep) projectFiles
    let isMainDotDaml = (== "Main.daml") . takeFileName
        sourceM = find isMainDotDaml projectFilesRel
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
runNew :: FilePath -> Maybe String -> [String] -> [String] -> IO ()
runNew targetFolder templateNameM pkgDeps dataDeps = do
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
            , "    " <> showCommandForUser "daml" ["init", targetFolder]
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
                , "    " <> showCommandForUser "daml" ["new", "myproject", projectName]
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
            , "    " <> showCommandForUser "daml" ["init", daRoot]
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
                   . replace "__DATA_DEPENDENCIES__" (unlines ["  - " <> dep | dep <- dataDeps])
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
    runNew targetFolder (Just "migrate") [] [pkgPath1Abs, pkgPath2Abs]

    -- Call damlc to create the upgrade source files.
    procConfig <- toAssistantCommand
        [ "damlc"
        , "migrate"
        , "--srcdir"
        , "daml"
        , "--project-root"
        , targetFolder
        , pkgPath1
        , pkgPath2
        ]
    runProcess_ procConfig

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
newtype JsonApiPort = JsonApiPort Int

navigatorPortNavigatorArgs :: NavigatorPort -> [String]
navigatorPortNavigatorArgs (NavigatorPort p) = ["--port", show p]

navigatorURL :: NavigatorPort -> String
navigatorURL (NavigatorPort p) = "http://localhost:" <> show p

withSandbox :: SandboxPort -> [String] -> (Process () () () -> IO a) -> IO a
withSandbox (SandboxPort port) args a = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "sandbox-logback.xml")
    withJar damlSdkJar [logbackArg] (["sandbox", "--port", show port] ++ args) $ \ph -> do
        putStrLn "Waiting for sandbox to start: "
        -- TODO We need to figure out what a sane timeout for this step.
        waitForConnectionOnPort (putStr "." *> threadDelay 500000) port
        a ph

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

damlSdkJarFolder :: FilePath
damlSdkJarFolder = "daml-sdk"

damlSdkJar :: FilePath
damlSdkJar = damlSdkJarFolder </> "daml-sdk.jar"

withJsonApi :: SandboxPort -> JsonApiPort -> [String] -> (Process () () () -> IO a) -> IO a
withJsonApi (SandboxPort sandboxPort) (JsonApiPort jsonApiPort) args a = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "json-api-logback.xml")
    let jsonApiArgs =
            ["--ledger-host", "localhost", "--ledger-port", show sandboxPort, "--http-port", show jsonApiPort] <> args
    withJar damlSdkJar [logbackArg] ("json-api":jsonApiArgs) $ \ph -> do
        putStrLn "Waiting for JSON API to start: "
        -- For now, we have a dummy authorization header here to wait for startup since we cannot get a 200
        -- response otherwise. We probably want to add some method to detect successful startup without
        -- any authorization
        let headers =
                [ ("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0")
                ] :: HTTP.RequestHeaders
        waitForHttpServer (putStr "." *> threadDelay 500000) ("http://localhost:" <> show jsonApiPort <> "/contracts/search") headers
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

runStart
    :: Maybe SandboxPort
    -> StartNavigator
    -> JsonApiConfig
    -> OpenBrowser
    -> Maybe String
    -> WaitForSignal
    -> SandboxOptions
    -> NavigatorOptions
    -> JsonApiOptions
    -> IO ()
runStart
  sandboxPortM
  (StartNavigator shouldStartNavigator)
  (JsonApiConfig mbJsonApiPort)
  (OpenBrowser shouldOpenBrowser)
  onStartM
  (WaitForSignal shouldWaitForSignal)
  (SandboxOptions sandboxOpts)
  (NavigatorOptions navigatorOpts)
  (JsonApiOptions jsonApiOpts)
  = withProjectRoot Nothing (ProjectCheck "daml start" True) $ \_ _ -> do
    let sandboxPort = fromMaybe defaultSandboxPort sandboxPortM
    projectConfig <- getProjectConfig
    darPath <- getDarPath
    mbScenario :: Maybe String <-
        requiredE "Failed to parse scenario" $
        queryProjectConfig ["scenario"] projectConfig
    doBuild
    let scenarioArgs = maybe [] (\scenario -> ["--scenario", scenario]) mbScenario
    withSandbox sandboxPort (darPath : scenarioArgs ++ sandboxOpts) $ \sandboxPh -> do
        withNavigator' sandboxPh sandboxPort navigatorPort navigatorOpts $ \navigatorPh -> do
            whenJust onStartM $ \onStart -> runProcess_ (shell onStart)
            when (shouldStartNavigator && shouldOpenBrowser) $
                void $ openBrowser (navigatorURL navigatorPort)
            withJsonApi' sandboxPh sandboxPort jsonApiOpts $ \jsonApiPh -> do
                when shouldWaitForSignal $
                  void $ waitAnyCancel =<< mapM (async . waitExitCode) [navigatorPh,sandboxPh,jsonApiPh]

    where
        navigatorPort = NavigatorPort 7500
        defaultSandboxPort = SandboxPort 6865
        withNavigator' sandboxPh =
            if shouldStartNavigator
                then withNavigator
                else (\_ _ _ f -> f sandboxPh)
        withJsonApi' sandboxPh sandboxPort args f =
            case mbJsonApiPort of
                Nothing -> f sandboxPh
                Just jsonApiPort -> withJsonApi sandboxPort jsonApiPort args f

data LedgerFlags = LedgerFlags
  { hostM :: Maybe String
  , portM :: Maybe Int
  , tokFileM :: Maybe FilePath
  }

getTokFromFile :: Maybe FilePath -> IO (Maybe Token)
getTokFromFile tokFileM = do
  case tokFileM of
    Nothing -> return Nothing
    Just tokFile -> do
      -- This postprocessing step which allows trailing newlines
      -- matches the behavior of the Scala token reader in
      -- com.digitalasset.auth.TokenHolder.
      tok <- intercalate "\n" . lines <$> readFileUTF8 tokFile
      return (Just (Token tok))

getHostAndPortDefaults :: LedgerFlags -> IO LedgerArgs
getHostAndPortDefaults LedgerFlags{hostM,portM,tokFileM} = do
    host <- fromMaybeM getProjectLedgerHost hostM
    port <- fromMaybeM getProjectLedgerPort portM
    tokM <- getTokFromFile tokFileM
    return LedgerArgs {..}


-- | Allocate project parties and upload project DAR file to ledger.
runDeploy :: LedgerFlags -> IO ()
runDeploy flags = do
    hp <- getHostAndPortDefaults flags
    putStrLn $ "Deploying to " <> show hp
    let flags' = flags
            { hostM = Just (host hp)
            , portM = Just (port hp) }
    runLedgerAllocateParties flags' []
    runLedgerUploadDar flags' Nothing
    putStrLn "Deploy succeeded."

newtype JsonFlag = JsonFlag { unJsonFlag :: Bool }

-- | Fetch list of parties from ledger.
runLedgerListParties :: LedgerFlags -> JsonFlag -> IO ()
runLedgerListParties flags (JsonFlag json) = do
    hp <- getHostAndPortDefaults flags
    unless json . putStrLn $ "Listing parties at " <> show hp
    xs <- Ledger.listParties hp
    if json then do
        TL.putStrLn . encodeToLazyText . toJSON $
            [ object
                [ "party" .= TL.toStrict (unParty party)
                , "display_name" .= TL.toStrict displayName
                , "is_local" .= isLocal
                ]
            | PartyDetails {..} <- xs
            ]
    else if null xs then
        putStrLn "no parties are known"
    else
        mapM_ print xs

-- | Allocate parties on ledger. If list of parties is empty,
-- defaults to the project parties.
runLedgerAllocateParties :: LedgerFlags -> [String] -> IO ()
runLedgerAllocateParties flags partiesArg = do
    parties <- if notNull partiesArg
        then pure partiesArg
        else getProjectParties
    hp <- getHostAndPortDefaults flags
    putStrLn $ "Checking party allocation at " <> show hp
    mapM_ (allocatePartyIfRequired hp) parties

-- | Allocate a party if it doesn't already exist (by display name).
allocatePartyIfRequired :: LedgerArgs -> String -> IO ()
allocatePartyIfRequired hp name = do
    partyM <- Ledger.lookupParty hp name
    party <- flip fromMaybeM partyM $ do
        putStrLn $ "Allocating party for '" <> name <> "' at " <> show hp
        Ledger.allocateParty hp name
    putStrLn $ "Allocated " <> show party <> " for '" <> name <> "' at " <> show hp

-- | Upload a DAR file to the ledger. (Defaults to project DAR)
runLedgerUploadDar :: LedgerFlags -> Maybe FilePath -> IO ()
runLedgerUploadDar flags darPathM = do
    hp <- getHostAndPortDefaults flags
    darPath <- flip fromMaybeM darPathM $ do
        doBuild
        getDarPath
    putStrLn $ "Uploading " <> darPath <> " to " <> show hp
    bytes <- BS.readFile darPath
    Ledger.uploadDarFile hp bytes
    putStrLn "DAR upload succeeded."

-- | Run navigator against configured ledger. We supply Navigator with
-- the list of parties from the ledger, but in the future Navigator
-- should fetch the list of parties itself.
runLedgerNavigator :: LedgerFlags -> [String] -> IO ()
runLedgerNavigator flags remainingArguments = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "navigator-logback.xml")
    hostAndPort <- getHostAndPortDefaults flags
    putStrLn $ "Opening navigator at " <> show hostAndPort
    partyDetails <- Ledger.listParties hostAndPort

    withTempDir $ \confDir -> do
        let navigatorConfPath = confDir </> "ui-backend.conf"
            navigatorArgs = concat
                [ ["server"]
                , [host hostAndPort, show (port hostAndPort)]
                , remainingArguments
                ]

        writeFileUTF8 navigatorConfPath (T.unpack $ navigatorConfig partyDetails)
        unsetEnv "DAML_PROJECT" -- necessary to prevent config contamination
        withJar damlSdkJar [logbackArg] ("navigator" : navigatorArgs ++ ["-c", confDir </> "ui-backend.conf"]) $ \ph -> do
            exitCode <- waitExitCode ph
            exitWith exitCode

  where
    navigatorConfig :: [PartyDetails] -> T.Text
    navigatorConfig partyDetails = T.unlines . concat $
        [ ["users", "  {"]
        , [ T.concat
            [ "    "
            , T.pack . show $
                if TL.null displayName
                    then unParty party
                    else displayName
            , " { party = "
            , T.pack (show (unParty party))
            , " }"
            ]
          | PartyDetails{..} <- partyDetails
          ]
        , ["  }"]
        ]

getDarPath :: IO FilePath
getDarPath = do
    projectName <- getProjectName
    projectVersion <- getProjectVersion
    return $ ".daml" </> "dist" </> projectName <> "-" <> projectVersion <> ".dar"

doBuild :: IO ()
doBuild = do
    procConfig <- toAssistantCommand ["build"]
    runProcess_ procConfig

getProjectConfig :: IO ProjectConfig
getProjectConfig = do
    projectPath <- required "Must be called from within a project" =<< getProjectPath
    readProjectConfig (ProjectPath projectPath)

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
    manager <- HTTP.newManager HTTP.defaultManagerSettings
    request <- HTTP.parseRequest $ "HEAD " <> url
    request <- pure request { HTTP.requestHeaders = headers }
    untilJust $ do
        r <- tryJust (\e -> guard (isIOException e || isHttpException e)) $ HTTP.httpNoBody request manager
        case r of
            Right resp
                | HTTP.statusCode (HTTP.responseStatus resp) == 200 -> pure $ Just ()
            _ -> sleep *> pure Nothing
    where isIOException e = isJust (fromException e :: Maybe IOException)
          isHttpException e = isJust (fromException e :: Maybe HTTP.HttpException)

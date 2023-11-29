-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms #-}

module DA.Daml.Assistant.Install
    ( InstallOptions (..)
    , InstallEnv (..)
    , Artifactory.ArtifactoryApiKey(..)
    , versionInstall
    , install
    , uninstallVersion
    , Artifactory.queryArtifactoryApiKey
    , pattern RawInstallTarget_Project
    ) where

import DA.Directory
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import qualified DA.Daml.Assistant.Install.Artifactory as Artifactory
import qualified DA.Daml.Assistant.Install.Github as Github
import DA.Daml.Assistant.Install.Path
import DA.Daml.Assistant.Install.Completion
import DA.Daml.Assistant.Version (getLatestSdkSnapshotVersion, getLatestReleaseVersion, UseCache (..))
import DA.Daml.Assistant.Cache (CacheTimeout (..))
import DA.Daml.Project.Consts
import DA.Daml.Project.Config
import Safe
import Data.List
import Conduit
import qualified Data.Conduit.List as List
import qualified Data.Conduit.Tar.Extra as Tar
import qualified Data.Conduit.Zlib as Zlib
import Data.Either.Extra
import qualified Data.SemVer as SemVer
import Network.HTTP.Simple
import qualified Data.ByteString as BS
import qualified Data.ByteString.UTF8 as BS.UTF8
import System.Environment
import System.Exit
import System.IO
import System.IO.Extra (writeFileUTF8)
import System.IO.Temp
import System.FileLock (withFileLock, withTryFileLock, SharedExclusive (Exclusive))
import System.FilePath
import System.Directory
import Control.Monad.Extra
import Control.Exception.Safe
import System.ProgressBar
import System.Info.Extra (isWindows)
import Options.Applicative.Extended (determineAuto)

-- unix specific
import System.PosixCompat.Types ( FileMode )
import System.PosixCompat.Files
    ( removeLink
    , createSymbolicLink
    , fileMode
    , getFileStatus
    , setFileMode
    , intersectFileModes
    , unionFileModes
    , ownerReadMode
    , ownerWriteMode
    , ownerExecuteMode
    , groupReadMode
    , groupExecuteMode
    , otherReadMode
    , otherExecuteMode)

data InstallEnv = InstallEnv
    { options :: InstallOptions
        -- ^ command-line options to daml install
    , targetVersionM :: Maybe SdkVersion
        -- ^ target install version
    , assistantVersion :: Maybe DamlAssistantSdkVersion
        -- ^ version of running daml assistant
    , damlPath :: DamlPath
        -- ^ path to install daml assistant
    , useCache :: UseCache
        -- ^ path to daml assistant cache
    , missingAssistant :: Bool
        -- ^ daml assistant is not installed in expected path.
    , installingFromOutside :: Bool
        -- ^ daml install is running from outside daml path
        -- (e.g. when running install script).
    , projectPathM :: Maybe ProjectPath
        -- ^ project path (for "daml install project")
    , artifactoryApiKeyM :: Maybe Artifactory.ArtifactoryApiKey
        -- ^ Artifactoyr API key used to fetch SDK EE tarball.
    , output :: String -> IO ()
        -- ^ output an informative message
    }

-- | Perform action unless user has passed --quiet flag.
unlessQuiet :: InstallEnv -> IO () -> IO ()
unlessQuiet InstallEnv{..} | QuietInstall b <- iQuiet options =
    unless b

-- | Set up .daml directory if it's missing.
setupDamlPath :: DamlPath -> IO ()
setupDamlPath (DamlPath path) = do
    createDirectoryIfMissing True (path </> "bin")
    createDirectoryIfMissing True (path </> "sdk")
    -- Ensure that the config file exists.
    unlessM (doesFileExist (path </> damlConfigName)) $ do
        writeFileUTF8 (path </> damlConfigName) defaultConfig
  where
    defaultConfig = unlines
        [ "update-check: never"
        , if isWindows
            then "auto-install: false"
            else "auto-install: true"
        ]

-- | Install (extracted) SDK directory to the correct place, after performing
-- a version sanity check. Then run the sdk install hook if applicable.
installExtracted :: InstallEnv -> SdkPath -> IO ()
installExtracted env@InstallEnv{..} sourcePath =
    wrapErr "Installing extracted SDK release." $ do
        sourceConfig <- readSdkConfig sourcePath
        sourceVersion <- fromRightM throwIO (sdkVersionFromSdkConfig sourceConfig)

        -- Check that source version matches expected target version.
        whenJust targetVersionM $ \targetVersion -> do
            unless (sourceVersion == targetVersion) $ do
                throwIO $ assistantErrorBecause
                    "SDK release version mismatch."
                    ("Expected " <> versionToText targetVersion
                    <> " but got version " <> versionToText sourceVersion)

        -- Set file mode of files to install.
        requiredIO "Failed to set file modes for extracted SDK files." $
            walkRecursive (unwrapSdkPath sourcePath) WalkCallbacks
                { walkOnFile = setSdkFileMode
                , walkOnDirectoryPost = \path ->
                    when (path /= addTrailingPathSeparator (unwrapSdkPath sourcePath)) $
                        setSdkFileMode path
                , walkOnDirectoryPre = const (pure ())
                }

        setupDamlPath damlPath

        let targetPath = defaultSdkPath damlPath sourceVersion

        whenM (doesDirectoryExist (unwrapSdkPath targetPath)) $ do

            -- Check for overwriting the running executable on Windows.
            asstExePath <- getExecutablePath
            when (isWindows && (unwrapSdkPath targetPath `isPrefixOf` asstExePath)) $ do
                hPutStrLn stderr $ unlines
                    [  "Failed to reinstall SDK version " <> versionToString sourceVersion
                    <> " because assistant version " <> versionToString sourceVersion
                    <> " is currently running."
                    ,  ""
                    ,  "Suggested fix:"
                    ,  "  - Go to https://github.com/digital-asset/daml/releases/latest"
                    ,  "  - Download and run Windows installer."
                    ,  "  - Reinstall the SDK from scratch."
                    ]
                exitFailure

            requiredIO "Failed to set file modes for SDK to remove." $ do
                setWritableRecursive (unwrapSdkPath targetPath)

        when (sourcePath /= targetPath) $ do -- should be true 99.9% of the time,
                                            -- but in that 0.1% this check prevents us
                                            -- from deleting the sdk we want to install
                                            -- just because it's already in the right place.
            requiredIO "Failed to remove existing SDK installation." $
                removePathForcibly (unwrapSdkPath targetPath)
                -- Always removePathForcibly to uniformize renameDirectory behavior
                -- between windows and unices.
            requiredIO "Failed to move extracted SDK release to final location." $
                moveDirectory (unwrapSdkPath sourcePath) (unwrapSdkPath targetPath)

        requiredIO "Failed to set file mode of installed SDK directory." $
            setSdkFileMode (unwrapSdkPath targetPath)

        when (shouldInstallAssistant env sourceVersion) $
            activateDaml env targetPath

installedAssistantPath :: DamlPath -> FilePath
installedAssistantPath damlPath =
    let damlName = if isWindows then "daml.cmd" else "daml"
    in unwrapDamlPath damlPath </> "bin" </> damlName

activateDaml :: InstallEnv -> SdkPath -> IO ()
activateDaml env@InstallEnv{..} targetPath = do

    let damlSourceName = if isWindows then "daml.exe" else "daml"
        damlBinarySourcePath = unwrapSdkPath targetPath </> "daml" </> damlSourceName
        damlBinaryTargetPath = installedAssistantPath damlPath
        damlBinaryTargetDir = takeDirectory damlBinaryTargetPath
        damlBinaryRelativeSourcePath = ".." </> makeRelative (unwrapDamlPath damlPath) damlBinarySourcePath

    unlessM (doesFileExist damlBinarySourcePath) $
        throwIO $ assistantErrorBecause
            "daml binary is missing from SDK release."
            ("expected path = " <> pack damlBinarySourcePath)

    whenM (doesFileExist damlBinaryTargetPath) $
        requiredIO "Failed to delete existing daml binary link." $
            if isWindows
                then removePathForcibly damlBinaryTargetPath
                else removeLink damlBinaryTargetPath

    requiredIO ("Failed to link daml binary in " <> pack damlBinaryTargetDir) $
        if isWindows
            then writeFile damlBinaryTargetPath $ unlines
                     [ "@echo off"
                     , "\"" <> damlBinarySourcePath <> "\" %*"
                     ]
            else createSymbolicLink damlBinaryRelativeSourcePath damlBinaryTargetPath

    updatePath options (\s -> unlessQuiet env (output s)) damlBinaryTargetDir
    installBashCompletions options damlPath (\s -> unlessQuiet env (output s))
    installZshCompletions options damlPath (\s -> unlessQuiet env (output s))

data WalkCallbacks = WalkCallbacks
    { walkOnFile :: FilePath -> IO ()
        -- ^ Callback to be called on files.
    , walkOnDirectoryPre  :: FilePath -> IO ()
        -- ^ Callback to be called on directories before recursion.
    , walkOnDirectoryPost :: FilePath -> IO ()
        -- ^ Callback to be called on directories after recursion.
    }

-- | Walk directory tree recursively, calling the callbacks specified in
-- the WalkCallbacks record. Each callback path is prefixed with the
-- query path. Directory callback paths have trailing path separator.
--
-- Edge case: If walkRecursive is called on a non-existant path, it will
-- call the walkOnFile callback.
walkRecursive :: FilePath -> WalkCallbacks -> IO ()
walkRecursive path callbacks = do
    isDirectory <- doesDirectoryExist path
    if isDirectory
        then do
            let dirPath = addTrailingPathSeparator path -- for uniformity
            walkOnDirectoryPre callbacks dirPath
            children <- listDirectory dirPath
            forM_ children $ \child -> do
                walkRecursive (dirPath </> child) callbacks
            walkOnDirectoryPost callbacks dirPath
        else do
            walkOnFile callbacks path

-- | Restrict file modes of installed sdk files to read and execute.
setSdkFileMode :: FilePath -> IO ()
setSdkFileMode path = do
    sourceMode <- fileMode <$> getFileStatus path
    setFileMode path (intersectFileModes fileModeMask sourceMode)

-- | Add write permissions to file or directory.
setWritable :: FilePath -> IO ()
setWritable path = do
    sourceMode <- fileMode <$> getFileStatus path
    setFileMode path (unionFileModes ownerWriteMode sourceMode)

setWritableRecursive :: FilePath -> IO ()
setWritableRecursive path =
    walkRecursive path WalkCallbacks
        { walkOnFile = setWritable
        , walkOnDirectoryPre = setWritable
        , walkOnDirectoryPost = const (pure ())
        }

-- | File mode mask to be applied to installed SDK files.
fileModeMask :: FileMode
fileModeMask = foldl1 unionFileModes
    [ ownerReadMode
    , ownerExecuteMode
    , groupReadMode
    , groupExecuteMode
    , otherReadMode
    , otherExecuteMode
    ]

-- | Copy an extracted SDK release directory and install it.
copyAndInstall :: InstallEnv -> FilePath -> IO ()
copyAndInstall env sourcePath =
    wrapErr "Copying SDK release directory." $ do
        withSystemTempDirectory "daml-update" $ \tmp -> do
            let copyPath = tmp </> "release"
                prefixLen = length (addTrailingPathSeparator sourcePath)
                newPath path = copyPath </> drop prefixLen path

            walkRecursive sourcePath WalkCallbacks
                { walkOnFile = \path -> copyFileWithMetadata path (newPath path)
                , walkOnDirectoryPre = \path -> createDirectory (newPath path)
                , walkOnDirectoryPost = \_ -> pure ()
                }

            installExtracted env (SdkPath copyPath)

-- | Extract a tarGz bytestring and install it.
extractAndInstall :: InstallEnv
    -> ConduitT () BS.ByteString (ResourceT IO) () -> IO ()
extractAndInstall env source =
    wrapErr "Extracting SDK release tarball." $ do
        withSystemTempDirectory "daml-update" $ \tmp -> do
            let extractPath = tmp </> "release"
            createDirectory extractPath
            runConduitRes
                $ source
                .| Zlib.ungzip
                .| Tar.untar (Tar.restoreFile throwError extractPath)
            installExtracted env (SdkPath extractPath)
    where throwError msg e = liftIO $ throwIO $ assistantErrorBecause ("Invalid SDK release: " <> msg) e

-- | Download an sdk tarball and install it.
httpInstall :: InstallEnv -> SdkVersion -> IO ()
httpInstall env@InstallEnv{..} version = do
    unlessQuiet env $ output "Downloading SDK release."
    requiredAny "Failed to download SDK release." $
        downloadLocation location
    where
        location = case artifactoryApiKeyM of
            Nothing -> Github.versionLocation version
            Just apiKey
              | version >= firstEEVersion -> Artifactory.versionLocation version apiKey
              | otherwise -> Github.versionLocation version
        !firstEEVersion =
            let verStr = "1.12.0-snapshot.20210312.6498.0.707c86aa"
            in SdkVersion (either error id (SemVer.fromText verStr))
        downloadLocation :: InstallLocation -> IO ()
        downloadLocation (InstallLocation url headers) = do
            request <- requiredAny "Failed to parse HTTPS request." $ parseRequest ("GET " <> unpack url)
            withResponse (setRequestHeaders headers request) $ \response -> do
                when (getResponseStatusCode response /= 200) $
                    throwIO . assistantErrorBecause "Failed to download release."
                            . pack . show $ getResponseStatus response
                let totalSizeM = readMay . BS.UTF8.toString =<< headMay (getResponseHeader "Content-Length" response)
                extractAndInstall env
                    . maybe id (\s -> (.| observeProgress s)) totalSizeM
                    $ getResponseBody response
        observeProgress :: MonadResource m =>
            Int -> ConduitT BS.ByteString BS.ByteString m ()
        observeProgress totalSize = do
            pb <- liftIO $ newProgressBar defStyle 10 (Progress 0 totalSize ())
            List.mapM $ \bs -> do
                liftIO $ incProgress pb (BS.length bs)
                pure bs

-- | Perform an action with a file lock from DAML_HOME/sdk/.lock
-- This function blocks until the lock has been obtained.
-- If the lock cannot be obtained immediately without blocking,
-- a message is output.
--
-- The lock is released after the action is performed, and is
-- automatically released if the process ends prematurely.
withInstallLock :: InstallEnv -> IO a -> IO a
withInstallLock InstallEnv{..} action = do
    let damlSdkPath = unwrapDamlPath damlPath </> "sdk"
        lockFilePath = damlSdkPath </> ".lock"
    createDirectoryIfMissing True damlSdkPath
    resultM <- withTryFileLock lockFilePath Exclusive (const action)
    case resultM of
        Just x -> pure x
        Nothing -> do
            output ("Waiting for SDK installation lock " <> lockFilePath)
            withFileLock lockFilePath Exclusive (const action)

-- | Install SDK from a path. If the path is a tarball, extract it first.
--
-- This function takes the install file lock, so it can't be performed
-- concurrently with a versionInstall or another pathInstall, blocking
-- until the other process is finished.
pathInstall :: InstallEnv -> FilePath -> IO ()
pathInstall env@InstallEnv{..} sourcePath = withInstallLock env $ do
    isDirectory <- doesDirectoryExist sourcePath
    if isDirectory
        then do
            unlessQuiet env $ output "Installing SDK release from directory."
            copyAndInstall env sourcePath
        else do
            unlessQuiet env $ output "Installing SDK release from tarball."
            extractAndInstall env (sourceFileBS sourcePath)

-- | Install a specific SDK version.
--
-- This function takes the install file lock, so it can't be performed
-- concurrently with a pathInstall or another versionInstall, blocking
-- until the other process is finished.
versionInstall :: InstallEnv -> SdkVersion -> IO ()
versionInstall env@InstallEnv{..} version = withInstallLock env $ do

    let SdkPath path = defaultSdkPath damlPath version
    alreadyInstalled <- doesDirectoryExist path

    let forceFlag = unForceInstall (iForce options)
        performInstall = not alreadyInstalled || forceFlag

    unlessQuiet env . output . concat $
        if alreadyInstalled then
            [ "SDK version "
            , versionToString version
            , " is already installed."
            , if performInstall
                then " Reinstalling."
                else ""
            ]
        else
            [ "Installing SDK version "
            , versionToString version
            ]

    when performInstall $
        httpInstall env { targetVersionM = Just version } version

    -- Need to activate here if we aren't performing the full install.
    when (not performInstall && shouldInstallAssistant env version) $ do
        unlessQuiet env . output $
            "Activating assistant version " <> versionToString version
        activateDaml env (SdkPath path)

-- | Install the latest version of the SDK.
latestInstall :: InstallEnv -> IO ()
latestInstall env@InstallEnv{..} = do
    version1 <- getLatestReleaseVersion useCache
        -- override the cache if it's older than 1 day, even if daml-config.yaml says otherwise
        { overrideTimeout = Just (CacheTimeout 86400)
        }
    version2M <- if iSnapshots options
        then tryAssistantM $ getLatestSdkSnapshotVersion useCache
        else pure Nothing
    let version = maybe version1 (max version1) version2M
    versionInstall env version

-- | Install the SDK version of the current project.
projectInstall :: InstallEnv -> ProjectPath -> IO ()
projectInstall env projectPath = do
    projectConfig <- readProjectConfig projectPath
    versionM <- fromRightM throwIO $ sdkVersionFromProjectConfig projectConfig
    version <- required "SDK version missing from project config (daml.yaml)." versionM
    versionInstall env version

-- | Determine whether the assistant should be installed.
shouldInstallAssistant :: InstallEnv -> SdkVersion -> Bool
shouldInstallAssistant InstallEnv{..} versionToInstall =
    let isNewer = maybe True (< versionToInstall) (unwrapDamlAssistantSdkVersion <$> assistantVersion)
    in unActivateInstall (iActivate options)
    || determineAuto (isNewer || missingAssistant || installingFromOutside)
        (unwrapInstallAssistant (iAssistant options))

pattern RawInstallTarget_Project :: RawInstallTarget
pattern RawInstallTarget_Project = RawInstallTarget "project"

pattern RawInstallTarget_Latest :: RawInstallTarget
pattern RawInstallTarget_Latest = RawInstallTarget "latest"

-- | Run install command.
install :: InstallOptions -> DamlPath -> UseCache -> Maybe ProjectPath -> Maybe DamlAssistantSdkVersion -> IO ()
install options damlPath useCache projectPathM assistantVersion = do
    when (unActivateInstall (iActivate options)) $
        hPutStr stderr . unlines $
            [ "WARNING: --activate is deprecated, use --install-assistant=yes instead."
            , ""
            ]

    missingAssistant <- not <$> doesFileExist (installedAssistantPath damlPath)
    execPath <- getExecutablePath
    damlConfigE <- tryConfig $ readDamlConfig damlPath
    let installingFromOutside = not $
            isPrefixOf (unwrapDamlPath damlPath </> "") execPath
        targetVersionM = Nothing -- determined later
        output = putStrLn -- Output install messages to stdout.
        artifactoryApiKeyM = Artifactory.queryArtifactoryApiKey =<< eitherToMaybe damlConfigE
        env = InstallEnv {..}
    case iTargetM options of
        Nothing -> do
            hPutStrLn stderr $ unlines
                [ "ERROR: daml install requires a target."
                , ""
                , "Available install targets:"
                , "    daml install latest     Install the latest stable SDK version."
                , "    daml install project    Install the project SDK version."
                , "    daml install VERSION    Install a specific SDK version."
                , "    daml install PATH       Install SDK from an SDK release tarball."
                ]
            exitFailure

        Just RawInstallTarget_Project -> do
            projectPath <- required "'daml install project' must be run from within a project."
                projectPathM
            projectInstall env projectPath

        Just RawInstallTarget_Latest ->
            latestInstall env

        Just (RawInstallTarget arg) | Right version <- parseVersion (pack arg) ->
            versionInstall env version

        Just (RawInstallTarget arg) -> do
            testD <- doesDirectoryExist arg
            testF <- doesFileExist arg
            if testD || testF then
                pathInstall env arg
            else
                throwIO (assistantErrorBecause "Invalid install target. Expected version, path, 'project' or 'latest'." ("target = " <> pack arg))

-- | Uninstall a specific SDK version.
uninstallVersion :: Env -> SdkVersion -> IO ()
uninstallVersion Env{..} sdkVersion = wrapErr "Uninstalling SDK version." $ do
    let (SdkPath path) = defaultSdkPath envDamlPath sdkVersion

    exists <- doesDirectoryExist path
    if exists then do
        when (Just (DamlAssistantSdkVersion sdkVersion) == envDamlAssistantSdkVersion) $ do
            hPutStr stderr . unlines $
                [ "Cannot uninstall SDK version of current daml assistant."
                , "Please switch to a different version of daml assistant and try again."
                , "To switch to a different version of daml assistant, use:"
                , ""
                , "    daml install VERSION --install-assistant=yes"
                ] -- TODO: suggest a version that will work.
            exitFailure

        requiredIO "Failed to set write permission for SDK files to remove." $ do
            setWritableRecursive path

        requiredIO "Failed to remove SDK files." $ do
            removePathForcibly path

        putStrLn ("SDK version " <> versionToString sdkVersion <> " has been uninstalled.")

    else do
        putStrLn ("SDK version " <> versionToString sdkVersion <> " is not installed.")

-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DAML.Assistant.Install
    ( InstallOptions (..)
    , InstallURL (..)
    , installExtracted
    , extractAndInstall
    , httpInstall
    , pathInstall
    , targetInstallURL
    , defaultInstallURL
    , install
    ) where

import DAML.Project.Types
import DAML.Project.Consts
import DAML.Project.Util
import DAML.Project.Config
import Safe
import Conduit
import qualified Data.Conduit.List as List
import qualified Data.Conduit.Tar as Tar
import qualified Data.Conduit.Zlib as Zlib
import Network.HTTP.Simple
import qualified Data.ByteString as BS
import qualified Data.ByteString.UTF8 as BS.UTF8
import Data.List.Extra
import System.IO.Temp
import System.FilePath
import System.Directory
import Control.Monad.Extra
import Control.Exception.Safe
import System.ProgressBar
import System.Posix.Types
import System.Posix.Files -- forget windows for now
import qualified System.Info
import qualified Data.Text as T

displayInstallTarget :: InstallTarget -> Text
displayInstallTarget = \case
    InstallChannel (SdkChannel c) -> "channel " <> c
    InstallVersion (SdkVersion v) -> "version " <> v
    InstallPath p -> pack p

versionMatchesTarget :: SdkVersion -> InstallTarget -> Bool
versionMatchesTarget version = \case
    InstallChannel c -> c == fst (splitVersion version)
    InstallVersion v -> v == version
    InstallPath _ -> True -- tarball path could be any version

newtype InstallURL = InstallURL { unwrapInstallURL :: Text }

data SdkChannelInfo = SdkChannelInfo
    { channelName       :: SdkChannel
    , channelLatestURL  :: InstallURL
    , channelVersionURL :: SdkSubVersion -> InstallURL
    }

knownChannels :: [SdkChannelInfo]
knownChannels =
    [ SdkChannelInfo
        { channelName = SdkChannel "nightly"
        , channelLatestURL  = bintrayLatestURL
        , channelVersionURL = bintrayVersionURL
        }
    ]

data InstallEnv = InstallEnv
    { options :: InstallOptions
    , targetM :: Maybe InstallTarget
    , damlPath :: DamlPath
    }

-- | Perform action unless user has passed --force flag.
unlessForce :: InstallEnv -> IO () -> IO ()
unlessForce InstallEnv{..} | ForceInstall b <- iForce options =
    unless b

-- | Perform action unless user has passed --quiet flag.
unlessQuiet :: InstallEnv -> IO () -> IO ()
unlessQuiet InstallEnv{..} | QuietInstall b <- iQuiet options =
    unless b

-- | Execute action if --initial flag is set.
whenInitial :: InstallEnv -> IO () -> IO ()
whenInitial InstallEnv{..} | InitialInstall b <- iInitial options =
    when b

-- | Execute action if --activate flag is set.
whenActivate :: InstallEnv -> IO () -> IO ()
whenActivate InstallEnv{..} | ActivateInstall b <- iActivate options =
    when b


lookupChannel :: SdkChannel -> [SdkChannelInfo] -> Maybe SdkChannelInfo
lookupChannel ch = find ((== ch) . channelName)

targetInstallURL :: InstallTarget -> Maybe InstallURL
targetInstallURL = \case
    InstallChannel ch -> channelLatestURL <$> lookupChannel ch knownChannels
    InstallVersion v | (ch, sv) <- splitVersion v ->
        flip channelVersionURL sv <$> lookupChannel ch knownChannels
    InstallPath _ -> Nothing

defaultInstallURL :: InstallURL
defaultInstallURL = bintrayLatestURL

osName :: Text
osName = case System.Info.os of
    "darwin"  -> "osx"
    "linux"   -> "linux"
    "mingw32" -> "win"
    p -> error ("daml: Unknown operating system " ++ p)



bintrayVersionURL :: SdkSubVersion -> InstallURL
bintrayVersionURL (SdkSubVersion subVersion) = InstallURL $ T.concat
    [ "https://bintray.com/api/v1/content"  -- api call
    , "/digitalassetsdk/DigitalAssetSDK"    -- repo/subject
    , "/com/digitalasset/sdk-tarball/"      -- file path
    , subVersion
    , "/sdk-tarball-"
    , subVersion
    , "-"
    , osName
    , ".tar.gz"
    , "?bt_package=sdk-components"          -- package
    ]

bintrayLatestURL :: InstallURL
bintrayLatestURL = bintrayVersionURL (SdkSubVersion "$latest")

-- | Install (extracted) SDK directory to the correct place, after performing
-- a version sanity check. Then run the sdk install hook if applicable.
installExtracted :: InstallEnv -> SdkPath -> IO ()
installExtracted env@InstallEnv{..} sourcePath =
    wrapErr "Installing extracted SDK tarball." $ do
        sourceConfig <- readSdkConfig sourcePath
        sourceVersion <- fromRightM throwIO (sdkVersionFromSdkConfig sourceConfig)


        whenJust targetM $ \target ->
            unless (versionMatchesTarget sourceVersion target) $
                throwIO (assistantErrorBecause "SDK release version mismatch."
                    ("Expected " <> displayInstallTarget target
                    <> " but got version " <> unwrapSdkVersion sourceVersion))

        -- Set file mode of files to install.
        requiredIO "Failed to set file modes for extracted SDK files." $
            walkRecursive (unwrapSdkPath sourcePath) WalkCallbacks
                { walkOnFile = setSdkFileMode
                , walkOnDirectoryPost = \path ->
                    when (path /= addTrailingPathSeparator (unwrapSdkPath sourcePath)) $
                        setSdkFileMode path
                , walkOnDirectoryPre = \_ -> pure ()
                }

        let targetPath = defaultSdkPath damlPath sourceVersion
        when (sourcePath /= targetPath) $ do -- should be true 99.9% of the time,
                                            -- but in that 0.1% this check prevents us
                                            -- from deleting the sdk we want to install
                                            -- just because it's already in the right place.
            requiredIO "Failed to remove existing SDK installation." $
                removePathForcibly (unwrapSdkPath targetPath)
                -- Always removePathForcibly to uniformize renameDirectory behavior
                -- between windows and unices. (This is the wrong place for a --force check.
                -- That should occur before downloading or extracting any tarball.)
            requiredIO "Failed to move extracted SDK release to final location." $
                renameDirectory (unwrapSdkPath sourcePath) (unwrapSdkPath targetPath)

        requiredIO "Failed to set file mode of installed SDK directory." $
            setSdkFileMode (unwrapSdkPath targetPath)

        whenActivate env $ do
            let damlBinarySourcePath = unwrapSdkPath targetPath </> "daml" </> "daml"
                damlBinaryTargetDir  = unwrapDamlPath damlPath </> "bin"
                damlBinaryTargetPath = damlBinaryTargetDir </> "daml"

            unlessM (doesFileExist damlBinarySourcePath) $
                throwIO $ assistantErrorBecause
                    "daml binary is missing from SDK release."
                    ("expected path = " <> pack damlBinarySourcePath)

            whenM (doesFileExist damlBinaryTargetPath) $
                requiredIO "Failed to delete existing daml binary symbolic link." $
                    removeLink damlBinaryTargetPath

            requiredIO ("Failed to link daml binary in " <> pack damlBinaryTargetDir) $
                createSymbolicLink damlBinarySourcePath damlBinaryTargetPath

            unlessQuiet env $ do -- Ask user to add .daml/bin to PATH if it is absent.
                searchPaths <- map dropTrailingPathSeparator <$> getSearchPath
                when (damlBinaryTargetDir `notElem` searchPaths) $ do
                    putStrLn ("Please add " <> damlBinaryTargetDir <> " to your PATH.")

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
                .| Tar.untar (restoreFile extractPath)
            installExtracted env (SdkPath extractPath)
    where
        restoreFile :: MonadResource m => FilePath -> Tar.FileInfo
            -> ConduitT BS.ByteString Void m ()
        restoreFile extractPath info = do
            let oldPath = Tar.decodeFilePath (Tar.filePath info)
                newPath = dropDirectory1 oldPath
                targetPath = extractPath </> dropTrailingPathSeparator newPath
                parentPath = takeDirectory targetPath

            when (notNull newPath) $ do
                case Tar.fileType info of
                    Tar.FTNormal -> do
                        liftIO $ createDirectoryIfMissing True parentPath
                        sinkFileBS targetPath
                        liftIO $ setFileMode targetPath (Tar.fileMode info)
                    Tar.FTDirectory -> do
                        liftIO $ createDirectoryIfMissing True targetPath
                    unsupported ->
                        liftIO $ throwIO $ assistantErrorBecause
                            "Invalid SDK release: unsupported file type."
                            ("type = " <> pack (show unsupported) <>  ", path = " <> pack oldPath)

        -- | Drop first component from path
        dropDirectory1 :: FilePath -> FilePath
        dropDirectory1 = joinPath . tail . splitPath

-- | Download an sdk tarball and install it.
httpInstall :: InstallEnv -> InstallURL -> IO ()
httpInstall env (InstallURL url) = do
    unlessQuiet env $ putStrLn "Downloading SDK release."
    request <- parseRequest ("GET " <> unpack url)
    withResponse request $ \response -> do
        when (getResponseStatusCode response /= 200) $
            throwIO . assistantErrorBecause "Failed to download release."
                    . pack . show $ getResponseStatus response
        let totalSizeM = readMay . BS.UTF8.toString =<< headMay
                (getResponseHeader "Content-Length" response)
        extractAndInstall env
            . maybe id (\s -> (.| observeProgress s)) totalSizeM
            $ getResponseBody response
    where
        observeProgress :: MonadResource m =>
            Int -> ConduitT BS.ByteString BS.ByteString m ()
        observeProgress totalSize = do
            pb <- liftIO $ newProgressBar defStyle 10 (Progress 0 totalSize ())
            List.mapM $ \bs -> do
                liftIO $ incProgress pb (BS.length bs)
                pure bs

-- | Install SDK from a path. If the path is a tarball, extract it first.
pathInstall :: InstallEnv -> FilePath -> IO ()
pathInstall env sourcePath = do
    isDirectory <- doesDirectoryExist sourcePath
    if isDirectory
        then do
            unlessQuiet env $ putStrLn "Installing SDK release from directory."
            copyAndInstall env sourcePath
        else do
            unlessQuiet env $ putStrLn "Installing SDK release from tarball."
            extractAndInstall env (sourceFileBS sourcePath)

-- | Set up initial .daml directory.
initialInstall :: InstallEnv -> IO ()
initialInstall env@InstallEnv{..} = do
    let path = unwrapDamlPath damlPath
    whenM (doesDirectoryExist path) $ do
        unlessForce env $ do
            throwIO $ assistantErrorBecause
                ("DAML home directory " <> pack path <> " already exists. "
                    <> "Please remove it or use --force to continue.")
                ("path = " <> pack path)
    createDirectoryIfMissing True (path </> "bin")
    createDirectoryIfMissing True (path </> "sdk")
    -- For now, we only ensure that the file exists.
    appendFile (path </> damlConfigName) ""

-- | Disambiguate install target.
decideInstallTarget :: RawInstallTarget -> IO InstallTarget
decideInstallTarget (RawInstallTarget arg) = do
    testD <- doesDirectoryExist arg
    testF <- doesFileExist arg
    if testD || testF then
        pure (InstallPath arg)
    else if SdkChannel (pack arg) `elem` map channelName knownChannels then
        pure . InstallChannel . SdkChannel $ pack arg
    else
        pure . InstallVersion . SdkVersion $ pack arg

-- | Run install command.
install :: InstallOptions -> DamlPath -> IO ()
install options damlPath = do
    targetM <- mapM decideInstallTarget (iTargetM options)
    let env = InstallEnv {..}
    whenInitial env $ do
        initialInstall env

    case targetM of
        Nothing ->
            httpInstall env defaultInstallURL

        Just (InstallPath tarballPath) ->
            pathInstall env tarballPath

        Just (InstallChannel channel) -> do
            channelInfo <- required ("Unknown channel " <> unwrapSdkChannel channel) $
                lookupChannel channel knownChannels
            httpInstall env (channelLatestURL channelInfo)

        Just (InstallVersion version) -> do
            let (channel, subVersion) = splitVersion version
            channelInfo <- required ("Unknown channel " <> unwrapSdkChannel channel) $
                lookupChannel channel knownChannels
            httpInstall env (channelVersionURL channelInfo subVersion)

-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Module that works with "tar.gz" files.
module DA.Sdk.Shell.Tar
    ( UnTarGzipOptions(..)
    , defaultUnTarGzipOptions
    , unTarGzip
    , tarGzip
    ) where

import DA.Sdk.Prelude hiding (concat, displayException, stripPrefix)
import Conduit
import qualified Data.Conduit.List as CL
import Data.Text (unpack)
import Data.Conduit.Tar
import Data.Conduit.Zlib (gzip, ungzip)
import Data.Bifunctor
import Data.Text.Encoding
import Control.Exception
import Filesystem.Path.CurrentOS (concat, stripPrefix)
import System.Directory

-- | Options for the extracting of "tar.gz" archives.
newtype UnTarGzipOptions = UnTarGzipOptions
    { _stripComponents :: Maybe Int
      -- ^ Option passed to the "--strip-components" flag of "tar"
      -- Removes the specified number of leading path elements.
    }

defaultUnTarGzipOptions :: UnTarGzipOptions
defaultUnTarGzipOptions = UnTarGzipOptions {_stripComponents = Nothing}

-- | Extract files from a `tar.gz` archive.
unTarGzip ::
       UnTarGzipOptions
     -- ^ Untar mode
    -> FilePath
    -- ^ The archive to exract.
    -> FilePath
    -- ^ The folder it will be extracted in.
    -> IO (Either String ())
unTarGzip unTarGzOptions fromPath toPath = do
    errOrListWFilesAndExcs
        <- runConduitRes $ tryC $ sourceFileBS (pathToString fromPath)
                                .| ungzip
                                .| untarAndRestore
    case removeHarmlessErrors <$> errOrListWFilesAndExcs of
      Right []               -> return $ Right ()
      Right nonEmptyList     -> return $ maybe (Right ()) (Left . displayException) $ getFirstExcMb nonEmptyList
      Left err               -> return $ Left $ displayException (err :: SomeException)
  where
    -- Untars and restores files. It strips like `tar`:
    -- It removes the specified number of leading path elements.
    -- Pathnames with fewer elements will be silently skipped.
    untarAndRestore = untarWithExceptions
                        (\fInfo -> maybe
                                   (return ())                -- fewer elements, skip
                                   restoreFileIntoIfSupported -- no stripping, or enough elements and stripped, restore
                                   (stripFileInfoComponents fInfo))
    restoreFileIntoIfSupported fInfo =
        case fileType fInfo of
          FTNormal | filePath fInfo == "" ->
            return ()
          FTNormal ->
            defaultAction
          FTDirectory ->
            defaultAction
          FTSymbolicLink _ ->
            defaultAction
          unsupported  ->
            liftIO $ putStrLn
                ("[Warning] Unsupported file type skipped while extracting a tar package: " <>
                 pathStr  <> (" (Type: " <> show unsupported <> ")"))
      where
        pathStr = unpack $ decodeUtf8 $ filePath fInfo
        parentPath = pathToString $ parent (toPath </> stringToPath pathStr)
        defaultAction = do
            liftIO $ createDirectoryIfMissing True parentPath
            restoreFileIntoLenient (pathToString toPath) fInfo
    compCountToStrip                = fromMaybe 0 $ _stripComponents unTarGzOptions
    stripFileInfoComponents fInfo   = fmap (\fPathNew -> fInfo{ filePath = encodeUtf8 $ pathToText fPathNew })
                                           (stripComponents $ stringToPath $ getFileInfoPath fInfo)
    stripComponents fP = let components = splitDirectories fP
                         in if length components < compCountToStrip then Nothing
                            else Just $ concat $ drop compCountToStrip components
    getFirstExcMb fInfosWExcs = headMay $ concatMap snd fInfosWExcs
    removeHarmlessErrors = filter (\(_fI, excs) ->
                                   any (\ e -> not $ isHarmlessErrors e) excs)
    isHarmlessErrors e = let s = show e in
                         isPermissionSettingError s ||
                         isModificationTimeSettingError s
    isPermissionSettingError e =
        ("setOwnerAndGroup" `isInfixOf` e ||
         "setSymbolicLinkOwnerAndGroup" `isInfixOf` e) &&
        "permission denied (Operation not permitted)" `isSuffixOf` e
    isModificationTimeSettingError e =
        "setModificationTime" `isInfixOf` e &&
        "permission denied (Access is denied.)" `isSuffixOf` e

-- | Create a `tar.gz` archive.
-- We need to use the following options: "-czvf", pathToText toPath, "-C", pathToText fromPath, "."
tarGzip ::
       FilePath
    -- ^ The directory to create an archive from.
    -> FilePath
    -- ^ The output file (including extension).
    -> IO (Either String ())
tarGzip fromPath toPath = do
    errOrUnit
        <- runConduitRes $ tryC $ yield (pathToString fromPath)
                                .| void filePathConduit
                                .| CL.mapMaybe filePathToDirRelative
                                .| void tar
                                .| gzip
                                .| sinkFile (pathToString toPath)
    return $ first (displayException :: SomeException -> String) errOrUnit
  where
    filePathToDirRelative (Left fileInfo) = do
        let mbNewFilePath = toRelative $ getFileInfoPath fileInfo
        fmap (\new -> Left $ fileInfo{ filePath = new }) mbNewFilePath
    filePathToDirRelative (Right content) = Just $ Right content
    toRelative fPathAbsStr = let fPathAbs    = stringToPath fPathAbsStr
                                 fromPathDir = fromPath </> stringToPath ""
                             in   (encodeUtf8 . pathToText <$>
                                  (if fPathAbs == fromPath then Nothing
                                   else stripPrefix fromPathDir fPathAbs))

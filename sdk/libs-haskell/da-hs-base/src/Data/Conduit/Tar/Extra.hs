-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.Conduit.Tar.Extra
    ( module Data.Conduit.Tar
    , restoreFile
    , dropDirectory1
    ) where

import Conduit
import Control.Monad
import qualified Data.ByteString as BS
import Data.Conduit.Tar hiding (restoreFile)
import qualified Data.Conduit.Tar as Tar
import Data.List.Extra
import Data.Maybe
import Data.Text (Text, pack)
import System.Directory
import System.FilePath
import System.Info.Extra
import System.PosixCompat.Files (createSymbolicLink, setFileMode)


-- | This is intended to be used in combination with `Data.Conduit.Tar.untar`.
-- It writes the given file to the given directory stripping the first component
-- thereby emulating tar’s --strip-components=1 option.
restoreFile
    :: MonadResource m
    => (Text -> Text -> m ())
    -> FilePath
    -> Tar.FileInfo
    -> ConduitT BS.ByteString Void m ()
restoreFile throwError extractPath info = do
    let oldPath = Tar.decodeFilePath (Tar.filePath info)
        newPath = dropDirectory1 oldPath
        targetPath = extractPath </> dropTrailingPathSeparator newPath
        parentPath = takeDirectory targetPath

    when (pathEscapes newPath) $ do
        lift $ throwError
            "file path escapes tarball."
            ("path = " <> pack oldPath)

    when (notNull newPath) $ do
        case Tar.fileType info of
            Tar.FTNormal -> do
                liftIO $ createDirectoryIfMissing True parentPath
                sinkFileBS targetPath
                liftIO $ setFileMode targetPath (Tar.fileMode info)
            Tar.FTDirectory -> do
                liftIO $ createDirectoryIfMissing True targetPath
            Tar.FTSymbolicLink bs | not isWindows -> do
                let path = Tar.decodeFilePath bs
                unless (isRelative path) $
                    lift $ throwError
                        "symbolic link target is absolute."
                        ("target = " <> pack path <>  ", path = " <> pack oldPath)

                when (pathEscapes (takeDirectory newPath </> path)) $
                    lift $ throwError
                        "symbolic link target escapes tarball."
                        ("target = " <> pack path <> ", path = " <> pack oldPath)

                liftIO $ createDirectoryIfMissing True parentPath
                liftIO $ createSymbolicLink path targetPath
            unsupported ->
                lift $ throwError
                    "unsupported file type."
                    ("type = " <> pack (show unsupported) <>  ", path = " <> pack oldPath)

-- | Check whether a relative path escapes its root.
pathEscapes :: FilePath -> Bool
pathEscapes path = isNothing $ foldM step "" (splitDirectories path)
    where
        step acc "."  = Just acc
        step ""  ".." = Nothing
        step acc ".." = Just (takeDirectory acc)
        step acc name = Just (acc </> name)

-- | Drop first component from path
dropDirectory1 :: FilePath -> FilePath
dropDirectory1 = joinPath . tail . splitPath


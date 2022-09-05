-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Directory (copyDirectory, moveDirectory) where

import Control.Monad
import Control.Exception.Safe
import GHC.IO.Exception
import System.FilePath
import System.Directory.Extra
import System.IO.Error

copyDirectory :: FilePath -> FilePath -> IO ()
copyDirectory src target = do
    files <- listFilesRecursive src
    forM_ files $ \file -> do
        let baseName = makeRelative src file
        let targetFile = target </> baseName
        createDirectoryIfMissing True (takeDirectory targetFile)
        copyFile file targetFile

-- Similar to `renameDirectory` but falls back to a non-atomic copy + delete
-- if renameDirectory is unsupported, e.g., because src and target are on different
-- filesystems.
moveDirectory :: FilePath -> FilePath -> IO ()
moveDirectory src target =
    catchJust
        (\ex -> guard (ioeGetErrorType ex == UnsupportedOperation))
        (renameDirectory src target)
        (const $ do
             copyDirectory src target
             removePathForcibly src)

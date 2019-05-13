-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DAML.Project.Util
    ( fromRightM
    , fromMaybeM
    , copyDirectory
    , moveDirectory
    , ascendants
    ) where

import Control.Exception.Safe
import Control.Monad
import GHC.IO.Exception
import System.Directory.Extra
import System.FilePath
import System.IO.Error

-- | Same as 'fromRight' but monadic in the applied function.
fromRightM :: Applicative m => (a -> m b) -> Either a b -> m b
fromRightM f = either f pure

-- | Same as 'fromMaybe' but monadic in the default.
fromMaybeM :: Applicative m => m a -> Maybe a -> m a
fromMaybeM d = maybe d pure

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

-- | Calculate the ascendants of a path, i.e. the successive parents of a path,
-- including the path itself, all the way to its root. For example:
--
--     ascendants "/foo/bar/baz" == ["/foo/bar/baz", "/foo/bar", "/foo", "/"]
--     ascendants "~/foo/bar/baz" == ["~/foo/bar/baz", "~/foo/bar", "~/foo", "~"]
--     ascendants "./foo/bar/baz" == ["./foo/bar/baz", "./foo/bar", "./foo", "."]
--     ascendants "../foo/bar/baz" == ["../foo/bar/baz", "../foo/bar", "../foo", ".."]
--     ascendants "foo/bar/baz"  == ["foo/bar/baz", "foo/bar", "foo", "."]
--
ascendants :: FilePath -> [FilePath]
ascendants "" = ["."]
ascendants "~" = ["~"]
ascendants ".." = [".."]
ascendants p =
    let p' = takeDirectory (dropTrailingPathSeparator p)
        ps = if p == p' then [] else ascendants p'
    in p : ps


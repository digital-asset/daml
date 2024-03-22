-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Project.Util
    ( fromRightM
    , fromMaybeM
    , ascendants
    ) where

import System.FilePath

-- | Same as 'fromRight' but monadic in the applied function.
fromRightM :: Applicative m => (a -> m b) -> Either a b -> m b
fromRightM f = either f pure

-- | Same as 'fromMaybe' but monadic in the default.
fromMaybeM :: Applicative m => m a -> Maybe a -> m a
fromMaybeM d = maybe d pure

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

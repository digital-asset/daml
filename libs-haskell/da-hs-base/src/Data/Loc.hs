-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Source locations data type and helper functions.
--
-- The problem with Parsec's built-in source location infrastructure
-- is that the show instance is non pretty-printing compatible and
-- they implement no Read, JSON, etc.  They also hide the
-- implementation so it's hard to fix/workaround these issues.

module Data.Loc(
    Loc(..),
    ) where

-- | Source locations, we can't use Parsec's built-in source location,
-- because they implement a silly (non pretty-print compatible) Show
-- instance.
data Loc
    = Loc FilePath Int Int
    | NoLoc String -- reason for no location information
    deriving (Show)

-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Source locations data type and helper functions.
--
-- The problem with Parsec's built-in source location infrastructure
-- is that the show instance is non pretty-printing compatible and
-- they implement no Read, JSON, etc.  They also hide the
-- implementation so it's hard to fix/workaround these issues.

module Data.Loc where

import Control.Applicative

import Data.Aeson
import Data.Data (Data)
import GHC.Generics

import qualified "template-haskell" Language.Haskell.TH as TH

import Prelude (FilePath, Int, Read, Show, Ord, Eq, String, show)

-- | Source locations, we can't use Parsec's built-in source location,
-- because they implement a silly (non pretty-print compatible) Show
-- instance.
data Loc
    = Loc FilePath Int Int
    | NoLoc String -- reason for no location information
    deriving (Read, Data, Show, Ord, Eq, Generic)

-- defining the accessors outside, so our Show and Read instances are short
lFile :: Loc -> FilePath
lFile (Loc f _ _) = f
lFile (NoLoc _) = ""

lLine :: Loc -> Int
lLine (Loc _ l _) = l
lLine (NoLoc _) = -1

lCol :: Loc -> Int
lCol (Loc _ _ c) = c
lCol (NoLoc _) = -1

noLoc :: TH.ExpQ
noLoc = do
  l <- show <$> TH.location
  [| NoLoc l |]

instance ToJSON Loc
instance FromJSON Loc

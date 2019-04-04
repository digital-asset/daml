-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Prelude
    ( module DA.Sdk.Prelude.Path
    , module DA.Sdk.Prelude.IO
    , module Prelude

    -- TODO (SM): switch to 'located-base' for the 'xxxNote' versions
    , atMay        , atDef        , atNote
    , tailMay      , tailDef      , tailNote
    , initMay      , initDef      , initNote
    , headMay      , headDef      , headNote
    , lastMay      , lastDef      , lastNote
    , minimumMay   , minimumDef   , minimumNote
    , maximumMay   , maximumDef   , maximumNote
    , minimumByMay , minimumByDef , minimumByNote
    , maximumByMay , maximumByDef , maximumByNote
    , foldr1May    , foldr1Def    , foldr1Note
    , foldl1May'   , foldl1Def'   , foldl1Note'
    , scanl1May    , scanl1Def    , scanl1Note
    , scanr1May    , scanr1Def    , scanr1Note
    , assertNote
    , readMay      , readDef      , readNote
    , fromJustDef  , fromJustNote

    , for

    -- * Common exports from Data.*
    , module Data.Char
    , module Data.Function
    , module Data.Int
    , module Data.List.Extra
    , module Data.Maybe
    , module Data.Ord
    , module Data.Tuple
    , module Data.Void

    , Text

    , partitionEithers

    -- * Common exports from Control.*
    , module Control.Applicative
    , module Control.Monad
    , liftIO, MonadIO

    -- ** Monoid
    , Monoid(..)

    -- ** Tagged values
    , Tagged(Tagged, unTagged)

    -- Exception message
    , displayException
    ) where

import Data.Monoid
import Data.Either   (partitionEithers)
import Data.Tagged   (Tagged(Tagged, unTagged))
import Data.Char
import Data.Function
import Data.Maybe
import Data.Int
import Data.List.Extra hiding
    (
    -- hide partial functions
      minimumBy
    , maximumBy
    , foldl1
    , foldl1'

    -- foldl is almost always the wrong choice performance-wise.
    , foldl
    , for
    )
import Data.Ord
import Data.Tuple
import Data.Void
import Data.Text (Text)
import Data.Traversable (for)

import DA.Sdk.Prelude.Path
import DA.Sdk.Prelude.IO

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class (liftIO, MonadIO)
import qualified Control.Exception.Safe as Exc
import Type.Reflection                  as Refl

import Prelude hiding
    (
    -- hide partial functions
      tail
    , init
    , head
    , last
    , minimum
    , maximum
    , foldr1
    , scanl1
    , scanr1
    , read

    -- foldl is almost always the wrong choice performance-wise.
    , foldl

    -- We're using turtles `FilePath`
    , FilePath
    )

-- Total variants of partial functions. See above for the collection.
import Safe

displayException :: Exc.Exception e => e -> String
displayException err =
       "Description: "
    <> (if null libDisplayErr then "N/A" else libDisplayErr)
    <> "\nError type: "
    <> (show $ Refl.typeOf err)
  where
    libDisplayErr = Exc.displayException err

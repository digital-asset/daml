-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

-- | Custom Prelude without partial functions and a more extensive default
-- export list.
module DA.Prelude
  (
    -- * Prelude without partial functions
    module Prelude

    -- ** Total variants of partial functions

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

    -- * Common exports from Control.*
  , module Control.Applicative
  , module Control.Monad
  , liftIO

    -- * Common exports from Data.*
  , module Data.Char
  , module Data.Function
  , module Data.Int
  , module Data.List
  , module Data.List.Extra
  , module Data.Maybe
  , module Data.Ord
  , module Data.Proxy
  , module Data.Tuple
  , module Data.Void
  , module Data.Word

  , partitionEithers
  , singleton

  , Data
  , Typeable
  , Hashable

    -- ** Tagged values
  , Tagged(Tagged, unTagged)
  , retag
  , untag

    -- ** GHC Generics support
  , Generic

  , concatSequenceA
  , makeUnderscoreLenses
  ) where




-- orphan instances (all of them must be brought into scope here)
-----------------------------------------------------------------

import Orphans.Lib_aeson ()
import Orphans.Lib_binary ()
import Orphans.Lib_hashable ()


-- normal imports
-----------------

import Control.Applicative
import Control.Exception
import Control.Lens (set)
import Control.Monad
import Control.Monad.IO.Class (liftIO)

import Data.Char
import Data.Data     (Data)
import Data.Typeable (Typeable)
import Data.Either   (partitionEithers)

import Data.Function
import Data.Int
import Data.Hashable (Hashable)
import Data.List hiding
  (
    -- hide partial functions
    minimumBy
  , maximumBy
  , foldl1
  , foldl1'

    -- foldl and nub are almost always the wrong choice performance-wise.
  , foldl
  , nub
  )
import Data.List.Extra (nubOrd)
import Data.Maybe


import Data.Ord
import Data.Proxy
import Data.Tagged          (Tagged(Tagged, unTagged), retag, untag)
import Data.Tuple
import Data.Void
import Data.Word

import GHC.Generics (Generic)

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
  )

-- Total variants of partial functions. See above for the collection.
import Safe


-- qualified imports
--------------------

import qualified "template-haskell" Language.Haskell.TH        as TH
import qualified Control.Lens.TH            as Lens.TH


------------------------------------------------------------------------------
-- Support for fewer TH splices (speeds up GHCJS compilation)
------------------------------------------------------------------------------

-- | This function is mainly intended to be used to combine many TH-splices
-- into a single one, as we pay a per-splice cost of starting a node-js
-- executable and loading all libraries when compiling TH with GHCJS.
concatSequenceA :: (Applicative f) => [f [a]] -> f [a]
concatSequenceA = fmap concat . sequenceA


-- | Alternative name for 'pure' to make the intention of creating a singleton
-- container clearer at call sites. It can be used to create, e.g., singleton
-- lists or singleton 'Data.NonEmpty.NonEmpty' lists.
singleton :: Applicative f => a -> f a
singleton = pure

-- | Generate a lens for every field in a record. The name of the lens is the
-- name of the field prefixed by an underscore. For instance, for
--
-- > data Foo = Foo{bar :: Int, _baz :: Bool}
--
-- it will generate
--
-- > _bar :: Lens' Foo Int
-- > __baz :: Lens' Foo Bool
makeUnderscoreLenses :: TH.Name -> TH.DecsQ
makeUnderscoreLenses =
  Lens.TH.makeLensesWith (set Lens.TH.lensField noUnderscoreNoPrefixNamer Lens.TH.lensRules)
  where
    noUnderscoreNoPrefixNamer _ _ n = [Lens.TH.TopName (TH.mkName ('_':TH.nameBase n))]

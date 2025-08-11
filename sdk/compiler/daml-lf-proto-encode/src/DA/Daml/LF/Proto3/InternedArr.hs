-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

{-
This module containst the InternedArr datatype, used during the interning of
expressions. The implementation is hidden in this module, as a separate Num
tracks the length of the list. By encapsulating the constructor in here, we can
be sure the Num and List are not modified externally.

InternedArr, unlike InternedMap, implements sharing (i.e. in the case of the
same kind/type/expression occuring multiple times, sharing means having only one
entry in the table with multiple pointers to it versus duplicate entries).
-}

module DA.Daml.LF.Proto3.InternedArr (
  InternedArr
  ) where

import           Control.Monad.State.Strict

import           Data.Int
import qualified Data.Vector                as V

import qualified DA.Daml.LF.Proto3.Interned as G

data InternedArr val where
  InternedArr
      :: [val]
      -> !Int32 -- ^ the next available key
      -> InternedArr val

empty :: (Ord val) => InternedArr val
empty = InternedArr [] 0

toVec :: InternedArr val -> V.Vector val
toVec (InternedArr xs _) = V.fromList $ reverse xs

internState :: val -> State (InternedArr val) Int32
internState x = do
  (InternedArr xs n) <- get
  put $ InternedArr (x : xs) (n + 1)
  return n

instance G.Interned InternedArr where
  empty       = empty
  toVec       = toVec
  internState = internState

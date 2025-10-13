-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

{-
This module containst the InternedMap datatype, used during the interning of
kinds and types. The implementation is hidden in this module, as a separate Num
tracks the length of the list. By encapsulating the constructor in here, we can
be sure the Num and Map are not modified externally.

InternedMap, unlike InternedArr, does not implement sharing (i.e. in the case of
the same kind/type/expression occuring multiple times, sharing means having only
one entry in the table with multiple pointers to it versus duplicate entries).
-}

module DA.Daml.LF.Proto3.InternedMap (
  InternedMap
  ) where

import           Control.Monad.State.Strict

import           Data.Int
import qualified Data.List                  as L
import qualified Data.Map.Strict            as Map
import qualified Data.Vector                as V

import qualified DA.Daml.LF.Proto3.Interned as G

data InternedMap val where
  InternedMap
      :: (Ord val)
      => !(Map.Map val Int32)
      -> !Int32 -- ^ the next available key
      -> InternedMap val

empty :: (Ord val) => InternedMap val
empty = InternedMap Map.empty 0

toVec :: InternedMap val -> V.Vector val
toVec (InternedMap mp n) =
  let vec = V.fromList $ map fst $ L.sortOn snd $ Map.toList mp
  in
    if (fromIntegral $ V.length vec) == n
    then vec
    else error "internedKinds of incorrect length"

internState :: val -> State (InternedMap val) Int32
internState x = do
  (InternedMap mp n) <- get
  case x `Map.lookup` mp of
    Just m -> return m
    _ -> if n == maxBound
      then error "Interning table grew too large"
      else do
        put $! InternedMap (Map.insert x n mp) (n + 1)
        return n

instance G.Interned InternedMap where
  empty       = empty
  toVec       = toVec
  internState = internState

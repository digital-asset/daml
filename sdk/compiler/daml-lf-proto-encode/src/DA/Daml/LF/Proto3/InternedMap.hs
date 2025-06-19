-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

module DA.Daml.LF.Proto3.InternedMap (InternedMap, internState, toVec, empty) where

import           Control.Monad.State.Strict

import qualified Data.List           as L
import qualified Data.Map.Strict     as Map
import qualified Data.Vector         as V

data InternedMap val key where
  InternedMap :: (Ord val, Ord key, Num key, Bounded key) => !(Map.Map val key) -> !key -> InternedMap val key

empty :: (Ord val, Ord key, Num key, Bounded key) => InternedMap val key
empty = InternedMap Map.empty 0

toVec :: InternedMap val key -> V.Vector val
toVec (InternedMap mp n) =
  let vec = V.fromList $ map fst $ L.sortOn snd $ Map.toList mp
  in
    if (fromIntegral $ V.length vec) == n
    then vec
    else error "internedKinds of incorrect length"

extend :: val -> InternedMap val key -> (InternedMap val key, key)
extend x (InternedMap mp n) =
  if n == maxBound
  then error "Interning table grew too large"
  else (InternedMap (Map.insert x n mp) (n + 1), n)

-- API to be exposed
_internBlind :: val -> InternedMap val key -> (InternedMap val key, key)
_internBlind = extend

-- API to be exposed
internShare :: val -> InternedMap val key -> (InternedMap val key, key)
internShare x im@(InternedMap mp _) =
  case x `Map.lookup` mp of
    Just n -> (im, n)
    Nothing -> extend x im

_intern :: val -> InternedMap val key -> (InternedMap val key, key)
_intern = internShare

internState :: val -> State (InternedMap val key) key
internState x = do
  im@(InternedMap mp _) <- get
  case x `Map.lookup` mp of
    Just n -> return n
    Nothing -> do
      let (mp', n') = extend x im
      put mp'
      return n'

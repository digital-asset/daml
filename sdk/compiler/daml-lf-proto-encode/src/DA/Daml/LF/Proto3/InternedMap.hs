-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

{-
This module containst the InternedMap datatype, used during the interning of
kinds, types and expressions. The implementation is hidden in this module, as a
separate Num tracks the length of the list. By encapsulating the constructor in
here, we can be sure the Num and Map are not modified externally.
-}

module DA.Daml.LF.Proto3.InternedMap (
  InternedMap, internState, toVec, empty, extend
  ) where

import           Control.Monad.State.Strict

import qualified Data.List           as L
import qualified Data.Map.Strict     as Map
import qualified Data.Vector         as V

data InternedMap val key where
  InternedMap
      :: (Ord val, Ord key, Num key, Bounded key)
      => !(Map.Map val key)
      -> !key -- ^ the next available key
      -> InternedMap val key

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

internState :: val -> State (InternedMap val key) key
internState x = do
  im@(InternedMap mp _) <- get
  case x `Map.lookup` mp of
    Just n -> return n
    Nothing -> do
      let (mp', n') = extend x im
      put mp'
      return n'

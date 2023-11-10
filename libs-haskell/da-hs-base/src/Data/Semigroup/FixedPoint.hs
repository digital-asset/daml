-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.Semigroup.FixedPoint
  ( leastFixedPoint
  , leastFixedPointBy
  ) where

import           Data.Foldable (foldlM)
import Data.Graph qualified as G
import           Data.Hashable (Hashable)
import Data.HashMap.Strict qualified as HMS
import Data.HashSet qualified as HS
import           Data.List (foldl')

-- | When 'a' is a join-semilattice (i.e. a commutative idempotent
-- semigroup), @leastFixedPoint eqs@ computes the least (wrt. the partial order
-- induced by the semilattice) hashmap @h@ such that for each
-- @(x, a, [y_1, ..., y_n])@ in @eqs@ we have
--
-- > h ! x == a <> h ! y_1 <> ... <> h ! y_n
--
-- If there is a @y_i@ which does not appear among the @x@'s, @Left y_i@ is
-- returned.
leastFixedPoint
  :: (Ord x, Hashable x, Semigroup a)
  => [(x, a, [x])] -> Either x (HMS.HashMap x a)
leastFixedPoint = leastFixedPointBy (<>)

-- | Version of 'leastFixedPoint' which takes the operation of the
-- join-semilattice explicitly.
leastFixedPointBy
  :: (Ord x, Hashable x)
  => (a -> a -> a) -> [(x, a, [x])] -> Either x (HMS.HashMap x a)
leastFixedPointBy join eqs = do
  let sccs = G.stronglyConnCompR [(a,x,ys) | (x,a,ys) <- eqs]
  foldlM handleSCC HMS.empty sccs
  where
    lookupM acc x = maybe (Left x) pure (x `HMS.lookup` acc)
    handleSCC acc = \case
      G.AcyclicSCC (a, x, ys) -> do
        bs <- traverse (lookupM acc) ys
        let d = foldl' join a bs
        pure (HMS.insert x d acc)
      G.CyclicSCC [] -> pure acc  -- NOTE(MH): This should never happen.
      G.CyclicSCC ((a0, x0, ys0):(unzip3 -> (as, xs, yss))) -> do
        let zs = HS.fromList (concat (ys0:yss)) `HS.difference` HS.fromList (x0:xs)
        cs <- traverse (lookupM acc) (HS.toList zs)
        let d = foldl' join a0 (as ++ cs)
        pure (acc `HMS.union` HMS.fromList [(x,d) | x <- x0:xs])


-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.List.Extended
    ( spanMaybe
    , lookupWithIndex
    , module Data.List
    ) where

import Data.List

-- | Take from a list until a @Nothing@ is found, returning
-- the prefix of @Just@ values, and the rest of the list.
spanMaybe :: forall a b. (a -> Maybe b) -> [a] -> ([b], [a])
spanMaybe f = go []
    where
        go :: [b] -> [a] -> ([b],[a])
        go bs as
            | a:as' <- as
            , Just b <- f a
            = go (b:bs) as'

            | otherwise
            = (reverse bs, as)

-- | Find a key in an associative list and return its index and value.
lookupWithIndex :: Eq a => a -> [(a, b)] -> Maybe (Int, b)
lookupWithIndex = go 0
  where
    go !_ _ [] = Nothing
    go !n a0 ((a, b):abs)
        | a0 == a = Just (n, b)
        | otherwise = go (n+1) a0 abs

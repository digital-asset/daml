-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.List.Extended
    ( spanMaybe
    ) where

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


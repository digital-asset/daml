-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms #-}

module DA.Daml.LF.Ast.Range(
    Range(Inclusive, Empty),
    fromMinBound,
    fromMaxBound,
    elem,
    minBound,
    maxBound
    ) where

import Prelude hiding (elem, minBound, maxBound)
import qualified Prelude as P

data Range a = Inclusive_ a a | From a | Until a | Empty
    deriving (Eq, Show, Functor)

pattern Inclusive :: Ord a => a -> a -> Range a
pattern Inclusive low high <- Inclusive_ low high where
    Inclusive low high
        | high < low = error "ill-formed inclusive range"
        | otherwise = Inclusive_ low high

{-# COMPLETE Inclusive, From, Empty #-}

fromMinBound :: (Ord a, Bounded a) => a -> Range a
fromMinBound x = Inclusive x P.maxBound

fromMaxBound :: (Ord a, Bounded a) => a -> Range a
fromMaxBound x = Inclusive P.minBound x

elem :: Ord a => a -> Range a -> Bool
elem _ Empty = False
elem x (From low) = x >= low
elem x (Until high) = x <= high
elem x (Inclusive_ low high) = x >= low && x <= high

minBound :: Ord a => Range a -> Maybe a
minBound Empty = Nothing
minBound (Until _) = Nothing
minBound (From low) = Just low
minBound (Inclusive low _) = Just low

maxBound :: Ord a => Range a -> Maybe a
maxBound Empty = Nothing
maxBound (Until high) = Just high
maxBound (From _) = Nothing
maxBound (Inclusive _ high) = Just high

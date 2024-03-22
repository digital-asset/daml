-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms #-}

module DA.Daml.LF.Ast.Range(
    Range(Inclusive, Empty),
    elem,
    minBound,
    maxBound
    ) where

import Prelude hiding (elem, minBound, maxBound)

data Range a = Inclusive_ a a | Empty
    deriving (Eq, Show, Functor)

pattern Inclusive :: Ord a => a -> a -> Range a
pattern Inclusive low high <- Inclusive_ low high where
    Inclusive low high
        | high < low = error "ill-formed inclusive range"
        | otherwise = Inclusive_ low high

{-# COMPLETE Inclusive, Empty #-}

elem :: Ord a => a -> Range a -> Bool
elem _ Empty = False
elem x (Inclusive_ low high) = x >= low && x <= high

minBound :: Ord a => Range a -> Maybe a
minBound Empty = Nothing
minBound (Inclusive low _) = Just low

maxBound :: Ord a => Range a -> Maybe a
maxBound Empty = Nothing
maxBound (Inclusive _ high) = Just high
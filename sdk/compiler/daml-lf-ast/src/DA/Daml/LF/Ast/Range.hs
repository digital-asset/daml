-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms #-}

module DA.Daml.LF.Ast.Range(
    Range(Inclusive, From, Until, Empty),
    elem,
    minBound,
    maxBound
    ) where

import Prelude hiding (elem, minBound, maxBound)

import           DA.Pretty

data Range a = Inclusive_ a a | From a | Until a | Empty
    deriving (Eq, Show, Functor)

instance (Pretty a, Ord a) => Pretty (Range a) where
  pPrint = \case
    Empty -> string "empty"
    From low -> string "from" <> space <> pPrint low
    Until high -> string "until" <> space <> pPrint high
    Inclusive low high
        | low == high -> pPrint low
        | otherwise -> pPrint low <> space <> string "to" <> space <> pPrint high

pattern Inclusive :: Ord a => a -> a -> Range a
pattern Inclusive low high <- Inclusive_ low high where
    Inclusive low high
        | high < low = error "ill-formed inclusive range"
        | otherwise = Inclusive_ low high

{-# COMPLETE Inclusive, From, Until, Empty #-}

elem :: Ord a => a -> Range a -> Bool
elem _ Empty = False
elem x (From low) = x >= low
elem x (Until high) = x <= high
elem x (Inclusive_ low high) = x >= low && x <= high

minBound :: Ord a => Range a -> Maybe a
minBound Empty = Nothing
minBound (From low) = Just low
minBound (Until _) = Nothing
minBound (Inclusive low _) = Just low

maxBound :: Ord a => Range a -> Maybe a
maxBound Empty = Nothing
maxBound (From _) = Nothing
maxBound (Until high) = Just high
maxBound (Inclusive _ high) = Just high

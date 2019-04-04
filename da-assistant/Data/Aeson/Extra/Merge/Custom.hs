-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.Aeson.Extra.Merge.Custom
    (customMerge
    ) where

import Data.Aeson
import Data.Aeson.Extra.Merge
import Data.Align
import Data.These

-- | Merge two JSON values together
--
-- This is similar to @Data.Aeson.Extra.Merge@, the only difference
-- is that merging two @Array@s doesn't use @alignWith@ anymore;
-- instead, the second one is picked and the first is discarded in the
-- same way merging non-@Object@ values works.
customMerge :: Value -> Value -> Value
customMerge = merge alg
  where
    alg :: (a -> a -> a) -> ValueF a -> ValueF a -> ValueF a
    alg r a' b' = case (a', b') of
        (ObjectF a, ObjectF b) -> ObjectF $ alignWith f a b
        (_,         b)         -> b
      where
        f (These x y) = r x y
        f (This x)    = x
        f (That x)    = x

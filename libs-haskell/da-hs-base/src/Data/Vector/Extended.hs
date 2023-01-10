-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Data.Vector.Extended (
    module Data.Vector,
    constructNE,
) where

import Control.Monad.ST
import Data.Vector
import qualified Data.Vector.Mutable as M

-- | /O(n)/ Construct a vector with @n@ elements by repeatedly applying the
-- generator function to the already constructed part of the vector and the
-- index of the current element to construct.
constructNE :: forall a e. Int -> (Vector a -> Int -> Either e a) -> Either e (Vector a)
-- NOTE(MH): This is a copy of `Data.Vector.constructN` with small modifications
-- to pass the current index to `f` and to run in the `Either` monad.
constructNE !n f = runST $ do
    v  <- M.new n
    v' <- unsafeFreeze v
    fill v' 0
  where
    fill :: forall s. Vector a -> Int -> ST s (Either e (Vector a))
    fill !v i | i < n = case f (unsafeTake i v) i of
        Left e -> return (Left e)
        Right x -> seq x $ do
            v'  <- unsafeThaw v
            M.unsafeWrite v' i x
            v'' <- unsafeFreeze v'
            fill v'' (i+1)
    fill v _ = return (Right v)

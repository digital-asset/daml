-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeOperators #-}

module Data.HList (
        module Data.HList
    ) where

import Data.Kind (Type)
import Data.Functor.Contravariant

-- Allows us to create higher-order lists from tuples, relate them to Sums, and
-- relate them back to tuples
data Product (xs :: [Type]) where
  ProdZ :: Product '[]
  ProdT :: a -> Product xs -> Product (a ': xs)

data Sum (rest :: [Type]) where
  SumL :: a -> Sum (a ': xs)
  SumR :: Sum xs -> Sum (a ': xs)

toEither :: Sum (x ': y) -> Either x (Sum y)
toEither = eitherSum Left Right

fromEither :: Either x (Sum y) -> Sum (x ': y)
fromEither = either SumL SumR

eitherSum :: (l -> x) -> (Sum r -> x) -> Sum (l ': r) -> x
eitherSum lx _  (SumL l) = lx l
eitherSum _  rx (SumR r) = rx r

class SplitF f where
  splitF :: f (Either a b) -> (f a, f b)

class Split f xs where
  type SplitType f xs :: [Type]
  split :: f (Sum xs) -> Product (SplitType f xs)

instance Split f '[] where
  type SplitType f '[] = '[]
  split = const ProdZ

instance (SplitF f, Contravariant f, Split f rest) => Split f (a ': rest) where
  type SplitType f (a ': rest) = f a ': SplitType f rest
  split a = uncurry ProdT $ fmap split $ splitF $ contramap fromEither a

class CombineF f where
  combineF :: (f a, f b) -> f (Either a b)
  combineZ :: f a

class Combine f (xs :: [Type]) where
  type CombineType f xs :: [Type]
  combine :: Product (CombineType f xs) -> f (Sum xs)

instance (CombineF f) => Combine f '[] where
  type CombineType f '[] = '[]
  combine ProdZ = combineZ

instance (CombineF f, Contravariant f, Combine f xs) => Combine f (a ': xs) where
  type CombineType f (a ': xs) = f a ': CombineType f xs
  combine (ProdT left restSum) =
    let rest = combine restSum
    in
    contramap toEither $ combineF (left, rest)

{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeOperators #-}

module Data.HList where

import Data.Kind (Type)
import Data.Functor.Contravariant

-- Allows us to create higher-order lists from tuples, relate them to Sums, and
-- relate them back to tuples
data Product (xs :: [Type]) where
  ProdZ :: Product '[]
  ProdT :: a -> Product xs -> Product (a ': xs)

class ProductTupleIso xs tuple | xs -> tuple, tuple -> xs where
  toTuple :: Product xs -> tuple
  fromTuple :: tuple -> Product xs

instance ProductTupleIso '[x1, x2] (x1, x2) where
  toTuple (ProdT x1 (ProdT x2 ProdZ)) = (x1, x2)
  fromTuple (x1, x2) = ProdT x1 (ProdT x2 ProdZ)

instance ProductTupleIso '[x1, x2, x3] (x1, x2, x3) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 ProdZ))) = (x1, x2, x3)
  fromTuple (x1, x2, x3) = ProdT x1 (ProdT x2 (ProdT x3 ProdZ))

instance ProductTupleIso '[x1, x2, x3, x4] (x1, x2, x3, x4) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 ProdZ)))) = (x1, x2, x3, x4)
  fromTuple (x1, x2, x3, x4) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 ProdZ)))

instance ProductTupleIso '[x1, x2, x3, x4, x5] (x1, x2, x3, x4, x5) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 ProdZ))))) = (x1, x2, x3, x4, x5)
  fromTuple (x1, x2, x3, x4, x5) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 ProdZ))))

instance ProductTupleIso '[x1, x2, x3, x4, x5, x6] (x1, x2, x3, x4, x5, x6) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 ProdZ)))))) = (x1, x2, x3, x4, x5, x6)
  fromTuple (x1, x2, x3, x4, x5, x6) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 ProdZ)))))

instance ProductTupleIso '[x1, x2, x3, x4, x5, x6, x7] (x1, x2, x3, x4, x5, x6, x7) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 ProdZ))))))) = (x1, x2, x3, x4, x5, x6, x7)
  fromTuple (x1, x2, x3, x4, x5, x6, x7) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 ProdZ))))))

instance ProductTupleIso '[x1, x2, x3, x4, x5, x6, x7, x8] (x1, x2, x3, x4, x5, x6, x7, x8) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 ProdZ)))))))) = (x1, x2, x3, x4, x5, x6, x7, x8)
  fromTuple (x1, x2, x3, x4, x5, x6, x7, x8) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 ProdZ)))))))

instance ProductTupleIso '[x1, x2, x3, x4, x5, x6, x7, x8, x9] (x1, x2, x3, x4, x5, x6, x7, x8, x9) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 (ProdT x9 ProdZ))))))))) = (x1, x2, x3, x4, x5, x6, x7, x8, x9)
  fromTuple (x1, x2, x3, x4, x5, x6, x7, x8, x9) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 (ProdT x9 ProdZ))))))))

instance ProductTupleIso '[x1, x2, x3, x4, x5, x6, x7, x8, x9, x10] (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) where
  toTuple (ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 (ProdT x9 (ProdT x10 ProdZ)))))))))) = (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
  fromTuple (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) = ProdT x1 (ProdT x2 (ProdT x3 (ProdT x4 (ProdT x5 (ProdT x6 (ProdT x7 (ProdT x8 (ProdT x9 (ProdT x10 ProdZ)))))))))

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

combineFromTuple :: (ProductTupleIso (CombineType f xs) t, Combine f xs) => t -> f (Sum xs)
combineFromTuple = combine . fromTuple

splitToTuple :: (ProductTupleIso (SplitType f xs) t, Split f xs) => f (Sum xs) -> t
splitToTuple = toTuple . split

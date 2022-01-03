-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeOperators #-}
module Control.Lens.MonoTraversal
  ( MonoTraversable (..)
  ) where

import Control.Lens (Traversal')
import Data.Bitraversable
import GHC.Generics

-- | This class captures a traversal which is monomorphic in the following
-- sense: If we regard @a@ as a container type whose elements have type @e@,
-- then the @f@ in @'monoTraversal' f@ cannot change the type of the elements
-- and hence also not the type of the whole container.
class MonoTraversable e a where
  monoTraverse :: Applicative f => (e -> f e) -> a -> f a
  default monoTraverse
    :: (Generic a, GMonoTraversable e (Rep a)) => Traversal' a e
  monoTraverse f = fmap to . gmonoTraverse f . from

instance MonoTraversable e a => MonoTraversable e [a] where
  monoTraverse = traverse . monoTraverse

instance (MonoTraversable e a, MonoTraversable e b) => MonoTraversable e (a, b) where
  monoTraverse f = bitraverse (monoTraverse f) (monoTraverse f)

instance MonoTraversable e a => MonoTraversable e (Maybe a) where
  monoTraverse = traverse . monoTraverse

class GMonoTraversable e g where
  gmonoTraverse :: Traversal' (g p) e

instance GMonoTraversable e U1 where
  gmonoTraverse _ = pure

instance GMonoTraversable e V1 where
  gmonoTraverse _ = pure

instance MonoTraversable e a => GMonoTraversable e (K1 i a) where
  gmonoTraverse f (K1 x) = K1 <$> monoTraverse f x

instance GMonoTraversable e f => GMonoTraversable e (M1 i c f) where
  gmonoTraverse f (M1 x) = M1 <$> gmonoTraverse f x

instance (GMonoTraversable e f, GMonoTraversable e g) =>
         GMonoTraversable e (f :*: g) where
  gmonoTraverse f (x :*: y) = (:*:) <$> gmonoTraverse f x <*> gmonoTraverse f y

instance (GMonoTraversable e f, GMonoTraversable e g) =>
         GMonoTraversable e (f :+: g) where
  gmonoTraverse f = \case
    L1 x -> L1 <$> gmonoTraverse f x
    R1 y -> R1 <$> gmonoTraverse f y

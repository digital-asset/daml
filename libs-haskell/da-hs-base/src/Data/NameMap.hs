-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A map of named things with the following features:
--
-- 1. Things can be looked up in constant time by their names.
--
-- 2. Functions like 'insert' and 'fromList' fail when there are multiple
--    things with the same name. Higher order functions like 'map' and
--    'traverse' fail when the function passed results in multiple items with
--    the same name.
--
-- 3. Functions like 'toList', 'traverse' and the 'Foldable' methods yield the
--    things in the order they were inserted.
module Data.NameMap
  ( Named (..)
  , NameMap

  -- * Queries
  , null
  , names
  , namesSet
  , elems
  , size
  , member
  , lookup
  , (!)

  -- * Construction
  , empty
  , insert
  , insertEither
  , insertMany
  , insertManyEither
  , union

  -- * Transformations
  , map
  , traverse

  -- * Conversions
  , toList
  , singleton
  , fromList
  , fromListEither
  , toHashMap
  ) where

import Prelude hiding (lookup, map, traverse, null)
import qualified Prelude

import           Control.DeepSeq
import           Control.Monad (void)
import           Data.Aeson
import           Data.Binary
import           Data.Data
import           Data.Foldable hiding (toList, null)
import           Data.Function (on)
import           Data.Hashable
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Kind (Type)
import           GHC.Generics
import           GHC.Stack (HasCallStack)

-- | Type class for things that have a name.
class (Eq (Name a), Hashable (Name a), Show (Name a)) => Named a where
  type Name a :: Type
  name :: a -> Name a

-- NOTE(MH): We want to be able to convert to the underlying list without a
-- @Named a@ constraint. This is in particular necessary to get an instance
-- of 'Foldable' which runs through the elements in the right order. Hence we
-- store the @a@ twice. We also want to cache the @Name a@ for each @a@. The
-- 'index' is rebuild completely on 'map' and 'traverse'.
-- TODO(MH): Figure out a smarter a way of representing this.

-- | A map of named things.
data NameMap a = NameMap
  { _revAssocs :: ![(Name a, a)]
  , _index :: !(HMS.HashMap (Name a) a)
  }
  deriving (Generic)

instance (NFData a, NFData (Name a)) => NFData (NameMap a)

-- PRIMITIVES

names :: Named a => NameMap a -> [Name a]
names (NameMap ras _) = reverse (Prelude.map fst ras)

elems :: NameMap a -> [a]
elems (NameMap ras _ ) = reverse (Prelude.map snd ras)

toHashMap :: NameMap a -> HMS.HashMap (Name a) a
toHashMap (NameMap _ idx) = idx

empty :: NameMap a
empty = NameMap [] HMS.empty

-- | Insert a new thing into the map. Returns the name of the thing when
-- there is already a thing with the same name.
insertEither
  :: Named a
  => a -> NameMap a -> Either (Name a) (NameMap a)
insertEither x (NameMap ras0 idx0) = do
  let n = name x
  if n `HMS.member` idx0
    then Left n
    else do
      let ras1 = (n, x):ras0
      let idx1 = HMS.insert n x idx0
      pure (NameMap ras1 idx1)

-- | @traverse f m@ applies the function @f@ to all elements of @m@ in the
-- order they have been inserted into @m@. Fails when there are two elements
-- @x@, @y@ with @name (f x) == name (f y)@.
traverse :: (Applicative f, Named a, Named b) => (a -> f b) -> NameMap a -> f (NameMap b)
traverse f = fmap (errorOnDuplicate "traverse" . fromListEither) . Prelude.traverse f . toList

instance Foldable NameMap where
  foldr f z (NameMap ras _) = foldl' f' z ras
    where
      f' acc (_, x) = f x acc

-- DERIVED

null :: NameMap a -> Bool
null = HMS.null . toHashMap

namesSet :: Named a => NameMap a -> HS.HashSet (Name a)
namesSet = HS.fromMap . void . toHashMap

size :: NameMap a -> Int
size = HMS.size . toHashMap

-- | All elements of the map in the order they have been inserted.
toList :: NameMap a -> [a]
toList = elems

-- | Insert a new thing into the map. Fails when there is already a thing with
-- the same name.
insert :: (HasCallStack, Named a) => a -> NameMap a -> NameMap a
insert x tab = errorOnDuplicate "insert" $ insertEither x tab

insertManyEither :: Named a => [a] -> NameMap a -> Either (Name a) (NameMap a)
insertManyEither xs tab = foldlM (flip insertEither) tab xs

insertMany :: (HasCallStack, Named a) => [a] -> NameMap a -> NameMap a
insertMany xs tab = errorOnDuplicate "insertMany" $ insertManyEither xs tab

fromListEither :: Named a => [a] -> Either (Name a) (NameMap a)
fromListEither xs = insertManyEither xs empty

fromList :: (HasCallStack, Named a) => [a] -> NameMap a
fromList xs = errorOnDuplicate "fromList" $ fromListEither xs

singleton :: (HasCallStack, Named a) => a -> NameMap a
singleton x = fromList [x]

member :: Named a => Name a -> NameMap a -> Bool
member n = HMS.member n . toHashMap

lookup :: Named a => Name a -> NameMap a -> Maybe a
lookup n = HMS.lookup n . toHashMap

union :: Named a => NameMap a -> NameMap a -> NameMap a
union (NameMap _ nm1) (NameMap _ nm2) =
    let m = nm1 `HMS.union` nm2
     in NameMap (HMS.toList m) m

(!) :: (HasCallStack, Named a) => NameMap a -> Name a -> a
(!) nm n = case lookup n nm of
  Nothing -> error ("NameMap.!: Could not find name " ++ show n)
  Just x -> x

-- | @map f m@ applies the function @f@ to all elements of @m@.
-- Fails when there are two elements @x@, @y@ with @name (f x) == name (f y)@.
map :: Named a => (a -> a) -> NameMap a -> NameMap a
map f = errorOnDuplicate "map" . fromListEither . Prelude.map f . toList

errorOnDuplicate :: (HasCallStack, Named a) => String -> Either (Name a) (NameMap a) -> NameMap a
errorOnDuplicate fun = \case
  Left n -> error $ "Data.NameMap." ++ fun ++ ": duplicate name " ++ show n
  Right tab -> tab

instance (Named a, Eq a) => Eq (NameMap a) where
  (==) = (==) `on` toList

instance (Named a, Show a) => Show (NameMap a) where
  show = showString "NameMap " . show . toList

instance ToJSON a => ToJSON (NameMap a) where
  toJSON = toJSON . toList

instance (Named a, Binary a) => Binary (NameMap a) where
  put tab = put (size tab) *> traverse_ put (elems tab)
  get = fromList <$> get

deriving instance (Data a, Data (Name a), Named a) => Data (NameMap a)

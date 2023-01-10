-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Representation of Daml-LF type-level naturals.
module DA.Daml.LF.Ast.TypeLevelNat
    ( TypeLevelNat
    , TypeLevelNatError (..)
    , pattern TypeLevelNat10
    , fromTypeLevelNat
    , typeLevelNatE
    , typeLevelNat
    ) where

import Control.DeepSeq
import Data.Data
import Data.Hashable
import Data.Maybe
import Numeric.Natural
import GHC.Generics (Generic)

-- | A type-level natural. For now these are restricted to being between
-- 0 and 37 (inclusive). We do not expose the constructor of this type
-- to prevent the construction of values outside of that bound.
newtype TypeLevelNat
    = TypeLevelNat { unTypeLevelNat :: Int }
    deriving newtype (Eq, NFData, Ord, Show, Hashable)
    deriving (Data, Generic)

data TypeLevelNatError
    = TLNEOutOfBounds
    deriving (Eq, Ord, Show)

instance Bounded TypeLevelNat where
    minBound = TypeLevelNat 0
    maxBound = TypeLevelNat 37

fromTypeLevelNat :: Num b => TypeLevelNat -> b
fromTypeLevelNat = fromIntegral . unTypeLevelNat

-- | Construct a type-level natural in a safe way.
typeLevelNatE :: Integral a => a -> Either TypeLevelNatError TypeLevelNat
typeLevelNatE n'
    | n < fromTypeLevelNat minBound || n > fromTypeLevelNat maxBound = Left TLNEOutOfBounds
    | otherwise = Right $ TypeLevelNat (fromIntegral n)
  where
    n = fromIntegral n' :: Integer

-- | Construct a type-level natural. Raises an error if the number is out of bounds.
typeLevelNat :: Integral a => a -> TypeLevelNat
typeLevelNat m =
    case typeLevelNatE m of
        Left TLNEOutOfBounds -> error . concat $
            [ "type-level nat is out of bounds: "
            , show (fromIntegral m :: Integer)
            , " not in [0, "
            , show (maxBound @TypeLevelNat)
            , "]"
            ]
        Right n -> n


pattern TypeLevelNat10 :: TypeLevelNat
pattern TypeLevelNat10 = TypeLevelNat 10

instance Read TypeLevelNat where
    readsPrec p = mapMaybe postProcess . readsPrec p
      where
        postProcess :: (Natural, String) -> Maybe (TypeLevelNat, String)
        postProcess (m, xs) =
            case typeLevelNatE m of
                Left _ -> Nothing
                Right n -> Just (n, xs)

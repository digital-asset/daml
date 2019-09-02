-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | DAML-LF Numeric literals, with scale attached.
module DA.Daml.LF.Ast.Numeric
    ( Numeric
    , E10
    , numeric
    , numericScale
    , numericMaxScale
    , numericFromDecimal
    ) where

import Control.DeepSeq
import Control.Monad
import Data.Data
import Data.Decimal
import Data.Fixed
import Data.Maybe
import GHC.Generics (Generic)
import Numeric.Natural

-- | Numeric literal. This must encode both the mantissa (up to 38 digits) and
-- the scale (0-37), the latter controlling how many digits appear after the
-- decimal point. Furthermore, when reading or writing a Numeric value,
-- we need to show every significant digit after the decimal point, in order
-- to preserve the scale. For scale 0, the decimal point needs to be shown
-- without any following digits.
--
-- Internally we use Data.Decimal to represent these because it has the ability
-- to encode every Numeric alongside its scale, and it mostly does what we want
-- with Show and Read, with a few adjustments:
--
-- * we perform bounds checks with smart constructor 'numeric'
-- * for scale 0, we have to handle the decimal point in the Show
--   and Read instances manually
-- * when reading, we check numeric bounds for scale and mantissa
-- * we add Data, NFData, Generic instances
-- * we don't add Num instances (for now anyway)
--
newtype Numeric = Numeric { numericDecimal :: Decimal }
  deriving (Eq, Ord, Generic)

-- | Smart constructor for Numeric literals.
numeric :: Natural -> Integer -> Numeric
numeric s m
    | s > numericMaxScale = error "numeric error: scale too large"
    | m > numericMaxMantissa = error "numeric error: mantissa too large"
    | otherwise = Numeric $ Decimal (fromIntegral s) m

-- | Upper bound for numeric scale (inclusive).
numericMaxScale :: Natural
numericMaxScale = 37

-- | Upper bound for numeric mantissa (inclusive).
numericMaxMantissa :: Integer
numericMaxMantissa = 10^(38::Int)-1

-- | Get scale associated with numeric literal. This is the
-- number of decimal places after the decimal point. Ranges
-- between 0 and 'numericMaxScale' (inclusive).
numericScale :: Numeric -> Natural
numericScale = fromIntegral . decimalPlaces . numericDecimal

-- | Get mantissa associated with numeric literal. This is
-- the raw integer value of the numeric before adding the
-- decimal point. Ranges between 0 and 'numericMaxMantissa'
-- (inclusive).
numericMantissa :: Numeric -> Integer
numericMantissa = decimalMantissa . numericDecimal

-- | Fixed scale for (legacy) Decimal literals.
data E10
instance HasResolution E10 where
  resolution _ = 10000000000 -- 10^-10 resolution

-- | Convert a decimal literal into a numeric literal.
numericFromDecimal :: Fixed E10 -> Numeric
numericFromDecimal (MkFixed n) = numeric 10 n

instance Show Numeric where
    showsPrec p n
        | numericScale n == 0 = shows (numericDecimal n) . ("." ++)
        | otherwise = showsPrec p (numericDecimal n)

instance Read Numeric where
    readsPrec p = mapMaybe postProcess . readsPrec p
      where
        postProcess :: (Decimal, String) -> Maybe (Numeric, String)
        postProcess (d, xs) = do
            let n = Numeric d
            guard (numericValid n)
            if numericScale n > 0 then
                Just (n, xs)
            else -- for scale == 0, we have to take the decimal point manually
                case xs of
                    '.':ys -> Just (n, ys)
                    _ -> Nothing

        numericValid :: Numeric -> Bool
        numericValid n =
            numericScale n <= numericMaxScale
            && numericMantissa n <= numericMaxMantissa

instance Data Numeric where
    gfoldl k z n = z numeric `k` numericScale n `k` numericMantissa n
    gunfold k z _ = k (k (z numeric))
    dataTypeOf _ = tyNumeric
    toConstr _ = conNumeric

tyNumeric :: DataType
tyNumeric = mkDataType "DA.Daml.LF.Ast.Numeric.Numeric" [conNumeric]

conNumeric :: Constr
conNumeric = mkConstr tyNumeric "numeric" ["numericScale", "numericMantissa"] Prefix

instance NFData Numeric

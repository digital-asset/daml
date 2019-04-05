-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE MultiWayIf         #-}

-- | The model of our floating point decimal numbers.
module DA.Daml.LF.Decimal
    ( Decimal(..)
      -- * Functions needed by Core Package (Lexer, Arbitrary, Pretty...)
    , mkDecimal
      -- | used in Literal to parse Decimal literal
    , normalize
      -- | used here in Num instance for normalization after (+), (*)
      -- in mkDecimal to normalize constructed Decimal
    , divD
      -- | used here by roundD
    , roundD
      -- | used here by toIntegerD
    , toIntegerD
      -- | used in Time to convert Decimal to Rel Time (toRelTime)
    , fromIntegerD
      -- | used in Arbitrary to construct Decimal from integer
    , stringToDecimal
      -- | used in Pretty for Decimal to Doc conversion (prettyfication)
    , stringToScientific
      -- | used here by stringToDecimal
    , decimalToScientific
      -- | used here by decimalToScientific
    )
where
import           Control.Lens       (Iso', Prism', from, iso, prism')
import           Control.DeepSeq    (NFData(..))

import qualified Data.Aeson         as Aeson
import qualified Data.Scientific    as Scientific

import           DA.Prelude
import           Data.Ratio((%))


-- | A decimal `d` is represented as a tuple `(n :: Integer, k :: Natural)`
-- such that `d = n / 10 ^^ k = n * 10 ^^ -k`.
data Decimal = Decimal {_mantissa :: Integer, _exponent :: Integer}
  deriving (Generic, NFData, Show)


instance Num Decimal where
  (+) (Decimal n k) (Decimal m l) = let diff = k - l in
      if diff >= 0
        then normalize $ Decimal (n + m * 10 ^ diff) k
        else normalize $ Decimal (n * 10 ^ ( - diff) + m) l
  (*) (Decimal n k) (Decimal m l) = normalize $ Decimal (n * m) (k + l)
  abs (Decimal n k)               = Decimal (abs n) k
  signum (Decimal n _)            = Decimal (signum n) 0
  fromInteger n                   = Decimal n 0
  negate (Decimal n k)            = Decimal (negate n) k


instance Eq Decimal where
  (==) (Decimal n k) (Decimal m l) = let diff = k - l in
    if diff >= 0
      then n == m * 10 ^ diff
      else n * 10 ^ (-diff) == m


instance Ord Decimal where
  (<=) (Decimal n k) (Decimal m l) = let diff = k - l in
    if diff >= 0
      then n <= m * 10^diff
      else n * 10^(-diff) <= m


-- | JSON encoding compatible with Apollo (expecting an array)
instance Aeson.ToJSON Decimal where
    toJSON (Decimal m e) = Aeson.toJSON [m, e]


instance Aeson.FromJSON Decimal where
    parseJSON jsn = do
        [m, e] <- Aeson.parseJSON jsn
        return $ Decimal m e

-- The following are needed by core package
-------------------------------------------

-- | Constructor. Ensures the resulting `Decimal` is normalized.
mkDecimal :: Integer -> Integer -> Decimal
mkDecimal m e = normalize $ Decimal m e


-- | Normalize a Decimal. `normalize` returns the representation of the
--  `Decimal d = (n, k)` with the smallest possible absolute value of `k`. I.e.
--  `normalize (Decimal 10000 3) == Decimal 10 0`.
normalize :: Decimal -> Decimal
normalize d@(Decimal n k) | k > 0 = case divMod n 10 of
  (n', 0) -> normalize (Decimal n' (k-1))
  _       -> d
normalize (Decimal n k) = Decimal (n * 10 ^ (-k)) 0


-- | Rational division of two decimal numbers, followed by a `roundD` with
--   the given precision.
divD
  :: Integer      -- ^ The precision
  -> Decimal
  -> Decimal
  -> Decimal
divD prec (Decimal n1 k1) (Decimal n2 k2) = normalize res
  where
    -- combine all the powers of 10 into one exponent
    e = prec + k2 - k1;
    -- where we put the exponent depends on its sign
    d1' = if e >= 0 then n1 * 10 ^ e else n1
    d2' = if e >= 0 then n2 else n2 * 10 ^ (-e)
    f = d1'%d2'
    f' = floor f
    diff = f - fromIntegral f'
    res | diff > 1%2 || (diff == 1%2 && odd f') = Decimal (f' + 1) prec
        | otherwise = Decimal f' prec


-- | Round a decimal with 'Bankers' rounding mode. See
--   https://en.wikipedia.org/wiki/Rounding#Round_half_to_even
roundD :: Integer -> Decimal -> Decimal
roundD prec (Decimal n k) = divD prec (Decimal n k) (Decimal 1 0)


decimalToScientific :: Iso' Decimal Scientific.Scientific
decimalToScientific =
    iso there back
  where
    there (Decimal n k0)
      | k < toInteger (minBound :: Int) =
          error $ "decimalToScientific: underflow " <> show k
      | k > toInteger (maxBound :: Int) =
          error $ "decimalToScientific: overflow " <> show k
      | otherwise = Scientific.scientific n (fromInteger k)
      where
        k = negate k0

    back sc = Decimal (Scientific.coefficient sc)
                      (negate (toInteger (Scientific.base10Exponent sc)))


stringToScientific :: Prism' String Scientific.Scientific
stringToScientific =
    prism' (Scientific.formatScientific Scientific.Fixed Nothing) readMay


-- | Converts a `Decimal` to an `Integer` by truncating towards 0.
toIntegerD :: Decimal -> Integer
toIntegerD d = let
  (Decimal n _) = roundD 0 d
  d' = fromIntegerD n
  in if
    | d >= 0 && d' > d -> n - 1
    | d < 0 && d' < d -> n + 1
    | True -> n

-- | Converts an `Integer` to a `Decimal`.
fromIntegerD :: Integer -> Decimal
fromIntegerD n = Decimal n 0


stringToDecimal :: Prism' String Decimal
stringToDecimal = stringToScientific . from decimalToScientific


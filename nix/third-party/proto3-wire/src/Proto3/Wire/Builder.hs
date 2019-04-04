{-
  Copyright 2016 Awake Networks

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-}

-- | This module extends the "Data.ByteString.Builder" module by memoizing the
-- resulting length of each `Builder`
--
-- Example use:
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word32BE 42 <> charUtf8 'λ'))
-- [0,0,0,42,206,187]

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Proto3.Wire.Builder
    (
      -- * `Builder` type
      Builder

      -- * Create `Builder`s
    , byteString
    , lazyByteString
    , shortByteString
    , word8
    , word16BE
    , word16LE
    , word32BE
    , word32LE
    , word64BE
    , word64LE
    , int8
    , int16BE
    , int16LE
    , int32BE
    , int32LE
    , int64BE
    , int64LE
    , floatBE
    , floatLE
    , doubleBE
    , doubleLE
    , char7
    , string7
    , char8
    , string8
    , charUtf8
    , stringUtf8

      -- * Consume `Builder`s
    , builderLength
    , rawBuilder
    , toLazyByteString
    , hPutBuilder

    -- * Internal API
    , unsafeMakeBuilder
    ) where

import qualified Data.ByteString               as B
import qualified Data.ByteString.Builder       as BB
import qualified Data.ByteString.Builder.Extra as BB
import qualified Data.ByteString.Lazy          as BL
import qualified Data.ByteString.Short         as BS
import           Data.Char                     ( ord )
import           Data.Int                      ( Int8, Int16, Int32, Int64 )
import           Data.Semigroup                ( Semigroup(..), Sum(..) )
import           Data.Word                     ( Word8, Word16, Word32, Word64 )
import           System.IO                     ( Handle )

-- $setup
-- >>> :set -XOverloadedStrings
-- >>> import Data.Semigroup

-- | A `Builder` is like a @"Data.ByteString.Builder".`BB.Builder`@, but also
-- memoizes the resulting length so that we can efficiently encode nested
-- embedded messages.
--
-- You create a `Builder` by using one of the primitives provided in the
-- \"Create `Builder`s\" section.
--
-- You combine `Builder`s using the `Monoid` and `Semigroup` instances.
--
-- You consume a `Builder` by using one of the utilities provided in the
-- \"Consume `Builder`s\" section.
newtype Builder = Builder { unBuilder :: (Sum Word, BB.Builder) }
  deriving (Monoid, Semigroup)

instance Show Builder where
  showsPrec prec builder =
      showParen (prec > 10)
        (showString "Proto3.Wire.Builder.lazyByteString " . shows bytes)
    where
      bytes = toLazyByteString builder

-- | Retrieve the length of a `Builder`
--
-- > builderLength (x <> y) = builderLength x + builderLength y
-- >
-- > builderLength mempty = 0
--
-- >>> builderLength (word32BE 42)
-- 4
-- >>> builderLength (stringUtf8 "ABC")
-- 3
builderLength :: Builder -> Word
builderLength = getSum . fst . unBuilder

-- | Retrieve the underlying @"Data.ByteString.Builder".`BB.Builder`@
--
-- > rawBuilder (x <> y) = rawBuilder x <> rawBuilder y
-- >
-- > rawBuilder mempty = mempty
--
-- >>> Data.ByteString.Builder.toLazyByteString (rawBuilder (stringUtf8 "ABC"))
-- "ABC"
rawBuilder :: Builder -> BB.Builder
rawBuilder = snd . unBuilder

-- | Create a `Builder` from a @"Data.ByteString.Builder".`BB.Builder`@ and a
-- length.  This is unsafe because you are responsible for ensuring that the
-- provided length value matches the length of the
-- @"Data.ByteString.Builder".`BB.Builder`@
--
-- >>> unsafeMakeBuilder 3 (Data.ByteString.Builder.stringUtf8 "ABC")
-- Proto3.Wire.Builder.lazyByteString "ABC"
unsafeMakeBuilder :: Word -> BB.Builder -> Builder
unsafeMakeBuilder len bldr = Builder (Sum len, bldr)

-- | Create a lazy `BL.ByteString` from a `Builder`
--
-- > toLazyByteString (x <> y) = toLazyByteString x <> toLazyByteString y
-- >
-- > toLazyByteString mempty = mempty
--
-- >>> toLazyByteString (stringUtf8 "ABC")
-- "ABC"
toLazyByteString :: Builder -> BL.ByteString
toLazyByteString (Builder (Sum len, bb)) =
    BB.toLazyByteStringWith strat BL.empty bb
  where
    -- If the supplied length is accurate then we will perform just
    -- one allocation.  An inaccurate length would indicate a bug
    -- in one of the primitives that produces a 'Builder'.
    strat = BB.safeStrategy (fromIntegral len) BB.defaultChunkSize
{-# NOINLINE toLazyByteString #-}
  -- NOINLINE to avoid bloating caller; see docs for 'BB.toLazyByteStringWith'.

-- | Write a `Builder` to a `Handle`
--
-- > hPutBuilder handle (x <> y) = hPutBuilder handle x <> hPutBuilder handle y
-- >
-- > hPutBuilder handle mempty = mempty
--
-- >>> hPutBuilder System.IO.stdout (stringUtf8 "ABC\n")
-- ABC
hPutBuilder :: Handle -> Builder -> IO ()
hPutBuilder handle = BB.hPutBuilder handle . snd . unBuilder

-- | Convert a strict `B.ByteString` to a `Builder`
--
-- > byteString (x <> y) = byteString x <> byteString y
-- >
-- > byteString mempty = mempty
--
-- >>> byteString "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
byteString :: B.ByteString -> Builder
byteString bs =
  Builder (Sum (fromIntegral (B.length bs)), BB.byteStringCopy bs)
    -- NOTE: We want 'toLazyByteString' to produce a single chunk (unless
    -- incorrect uses of 'unsafeMakeBuilder' sabotage the length prediction).
    --
    -- To that end, 'toLazyByteString' allocates a first chunk of exactly the
    -- builder length.  That length should be accurate unless there is a bug,
    -- either within this library or in some arguments to 'unsafeMakeBuilder'.
    --
    -- If the given 'bs :: B.ByteString' is longer than a certain threshold,
    -- then passing it to 'BB.byteString' would produce a builder that closes
    -- the current chunk and appends 'bs' as its own chunk, without copying.
    -- That would waste some of the chunk allocated by 'toLazyByteString'.
    --
    -- Therefore we force copying of 'bs' by using 'BB.byteStringCopy' here.

-- | Convert a lazy `BL.ByteString` to a `Builder`
--
-- Warning: evaluating the length will force the lazy `BL.ByteString`'s chunks,
-- and they will remain allocated until you finish using the builder.
--
-- > lazyByteString (x <> y) = lazyByteString x <> lazyByteString y
-- >
-- > lazyByteString mempty = mempty
--
-- > lazyByteString . toLazyByteString = id
-- >
-- > toLazyByteString . lazyByteString = id
--
-- >>> lazyByteString "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
lazyByteString :: BL.ByteString -> Builder
lazyByteString bl =
  Builder (Sum (fromIntegral (BL.length bl)), BB.lazyByteStringCopy bl)
    -- NOTE: We use 'BB.lazyByteStringCopy' here for the same reason
    -- that 'byteString' uses 'BB.byteStringCopy'.  For the rationale,
    -- please see the comments in the implementation of 'byteString'.

-- | Convert a `BS.ShortByteString` to a `Builder`
--
-- > shortByteString (x <> y) = shortByteString x <> shortByteString y
-- >
-- > shortByteString mempty = mempty
--
-- >>> shortByteString "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
shortByteString :: BS.ShortByteString -> Builder
shortByteString bs =
  Builder (Sum (fromIntegral (BS.length bs)), BB.shortByteString bs)

-- | Convert a `Word8` to a `Builder`
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word8 42))
-- [42]
word8 :: Word8 -> Builder
word8 w = Builder (Sum 1, BB.word8 w)

-- | Convert a `Int8` to a `Builder`
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int8 (-5)))
-- [251]
int8 :: Int8 -> Builder
int8 w = Builder (Sum 1, BB.int8 w)

-- | Convert a `Word16` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word16BE 42))
-- [0,42]
word16BE :: Word16 -> Builder
word16BE w = Builder (Sum 2, BB.word16BE w)

-- | Convert a `Word16` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word16LE 42))
-- [42,0]
word16LE :: Word16 -> Builder
word16LE w = Builder (Sum 2, BB.word16LE w)

-- | Convert an `Int16` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int16BE (-5)))
-- [255,251]
int16BE :: Int16 -> Builder
int16BE w = Builder (Sum 2, BB.int16BE w)

-- | Convert an `Int16` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int16LE (-5)))
-- [251,255]
int16LE :: Int16 -> Builder
int16LE w = Builder (Sum 2, BB.int16LE w)

-- | Convert a `Word32` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word32BE 42))
-- [0,0,0,42]
word32BE :: Word32 -> Builder
word32BE w = Builder (Sum 4, BB.word32BE w)

-- | Convert a `Word32` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word32LE 42))
-- [42,0,0,0]
word32LE :: Word32 -> Builder
word32LE w = Builder (Sum 4, BB.word32LE w)

-- | Convert an `Int32` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int32BE (-5)))
-- [255,255,255,251]
int32BE :: Int32 -> Builder
int32BE w = Builder (Sum 4, BB.int32BE w)

-- | Convert an `Int32` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int32LE (-5)))
-- [251,255,255,255]
int32LE :: Int32 -> Builder
int32LE w = Builder (Sum 4, BB.int32LE w)

-- | Convert a `Float` to a `Builder` by storing the bytes in IEEE-754 format in
-- big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (floatBE 4.2))
-- [64,134,102,102]
floatBE :: Float -> Builder
floatBE f = Builder (Sum 4, BB.floatBE f)

-- | Convert a `Float` to a `Builder` by storing the bytes in IEEE-754 format in
-- little-endian order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (floatLE 4.2))
-- [102,102,134,64]
floatLE :: Float -> Builder
floatLE f = Builder (Sum 4, BB.floatLE f)

-- | Convert a `Word64` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word64BE 42))
-- [0,0,0,0,0,0,0,42]
word64BE :: Word64 -> Builder
word64BE w = Builder (Sum 8, BB.word64BE w)

-- | Convert a `Word64` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (word64LE 42))
-- [42,0,0,0,0,0,0,0]
word64LE :: Word64 -> Builder
word64LE w = Builder (Sum 8, BB.word64LE w)

-- | Convert an `Int64` to a `Builder` by storing the bytes in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int64BE (-5)))
-- [255,255,255,255,255,255,255,251]
int64BE :: Int64 -> Builder
int64BE w = Builder (Sum 8, BB.int64BE w)

-- | Convert an `Int64` to a `Builder` by storing the bytes in little-endian
-- order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (int64LE (-5)))
-- [251,255,255,255,255,255,255,255]
int64LE :: Int64 -> Builder
int64LE w = Builder (Sum 8, BB.int64LE w)

-- | Convert a `Double` to a `Builder` by storing the bytes in IEEE-754 format
-- in big-endian order
--
-- In other words, the most significant byte is stored first and the least
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (doubleBE 4.2))
-- [64,16,204,204,204,204,204,205]
doubleBE :: Double -> Builder
doubleBE f = Builder (Sum 8, BB.doubleBE f)

-- | Convert a `Double` to a `Builder` by storing the bytes in IEEE-754 format
-- in little-endian order
--
-- In other words, the least significant byte is stored first and the most
-- significant byte is stored last
--
-- >>> Data.ByteString.Lazy.unpack (toLazyByteString (doubleLE 4.2))
-- [205,204,204,204,204,204,16,64]
doubleLE :: Double -> Builder
doubleLE f = Builder (Sum 8, BB.doubleLE f)

-- | Convert an @ASCII@ `Char` to a `Builder`
--
-- __Careful:__ If you provide a Unicode character that is not part of the
-- @ASCII@ alphabet this will only encode the lowest 7 bits
--
-- >>> char7 ';'
-- Proto3.Wire.Builder.lazyByteString ";"
-- >>> char7 'λ' -- Example of truncation
-- Proto3.Wire.Builder.lazyByteString ";"
char7 :: Char -> Builder
char7 c = Builder (Sum 1, BB.char7 c)

-- | Convert an @ASCII@ `String` to a `Builder`
--
-- __Careful:__ If you provide a Unicode `String` that has non-@ASCII@
-- characters then this will only encode the lowest 7 bits of each character
--
-- > string7 (x <> y) = string7 x <> string7 y
-- >
-- > string7 mempty = mempty
--
-- >>> string7 "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
-- >>> string7 "←↑→↓" -- Example of truncation
-- Proto3.Wire.Builder.lazyByteString "\DLE\DC1\DC2\DC3"
string7 :: String -> Builder
string7 s = Builder (Sum (fromIntegral (length s)), BB.string7 s)

-- | Convert an @ISO/IEC 8859-1@ `Char` to a `Builder`
--
-- __Careful:__ If you provide a Unicode character that is not part of the
-- @ISO/IEC 8859-1@ alphabet then this will only encode the lowest 8 bits
--
-- >>> char8 ';'
-- Proto3.Wire.Builder.lazyByteString ";"
-- >>> char8 'λ' -- Example of truncation
-- Proto3.Wire.Builder.lazyByteString "\187"
char8 :: Char -> Builder
char8 c = Builder (Sum 1, BB.char8 c)

-- | Convert an @ISO/IEC 8859-1@ `String` to a `Builder`
--
-- __Careful:__ If you provide a Unicode `String` that has non-@ISO/IEC 8859-1@
-- characters then this will only encode the lowest 8 bits of each character
--
-- > string8 (x <> y) = string8 x <> string8 y
-- >
-- > string8 mempty = mempty
--
-- >>> string8 "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
-- >>> string8 "←↑→↓" -- Example of truncation
-- Proto3.Wire.Builder.lazyByteString "\144\145\146\147"
string8 :: String -> Builder
string8 s = Builder (Sum (fromIntegral (length s)), BB.string8 s)

-- | Convert a Unicode `Char` to a `Builder` using a @UTF-8@ encoding
--
-- >>> charUtf8 'A'
-- Proto3.Wire.Builder.lazyByteString "A"
-- >>> charUtf8 'λ'
-- Proto3.Wire.Builder.lazyByteString "\206\187"
-- >>> hPutBuilder System.IO.stdout (charUtf8 'λ' <> charUtf8 '\n')
-- λ
charUtf8 :: Char -> Builder
charUtf8 c = Builder (Sum (utf8Width c), BB.charUtf8 c)

-- | Convert a Unicode `String` to a `Builder` using a @UTF-8@ encoding
--
-- > stringUtf8 (x <> y) = stringUtf8 x <> stringUtf8 y
-- >
-- > stringUtf8 mempty = mempty
--
-- >>> stringUtf8 "ABC"
-- Proto3.Wire.Builder.lazyByteString "ABC"
-- >>> stringUtf8 "←↑→↓"
-- Proto3.Wire.Builder.lazyByteString "\226\134\144\226\134\145\226\134\146\226\134\147"
-- >>> hPutBuilder System.IO.stdout (stringUtf8 "←↑→↓\n")
-- ←↑→↓
stringUtf8 :: String -> Builder
stringUtf8 s = Builder (Sum (len 0 s), BB.stringUtf8 s)
  where
    len !n []      = n
    len !n (h : t) = len (n + utf8Width h) t
{-# INLINABLE stringUtf8 #-}
  -- INLINABLE so that if the input is constant, the
  -- compiler has the opportunity to precompute its length.

utf8Width :: Char -> Word
utf8Width c = case ord c of
  o | o <= 0x007F -> 1
    | o <= 0x07FF -> 2
    | o <= 0xFFFF -> 3
    | otherwise   -> 4
{-# INLINE utf8Width #-}

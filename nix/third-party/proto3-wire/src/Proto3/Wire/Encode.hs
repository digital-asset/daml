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

-- | Low level functions for writing the protobufs wire format.
--
-- Because protobuf messages are encoded as a collection of fields,
-- one can use the 'Monoid' instance for 'MessageBuilder' to encode multiple
-- fields.
--
-- One should be careful to make sure that 'FieldNumber's appear in
-- increasing order.
--
-- In protocol buffers version 3, all fields are optional. To omit a value
-- for a field, simply do not append it to the 'MessageBuilder'. One can
-- create functions for wrapping optional fields with a 'Maybe' type.
--
-- Similarly, repeated fields can be encoded by concatenating several values
-- with the same 'FieldNumber'.
--
-- For example:
--
-- > strings :: Foldable f => FieldNumber -> f String -> MessageBuilder
-- > strings = foldMap . string
-- >
-- > 1 `strings` Just "some string" <>
-- > 2 `strings` [ "foo", "bar", "baz" ]

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Proto3.Wire.Encode
    ( -- * `MessageBuilder` type
      MessageBuilder
    , messageLength
    , rawMessageBuilder
    , toLazyByteString
    , unsafeFromLazyByteString

      -- * Standard Integers
    , int32
    , int64
      -- * Unsigned Integers
    , uint32
    , uint64
      -- * Signed Integers
    , sint32
    , sint64
      -- * Non-varint Numbers
    , fixed32
    , fixed64
    , sfixed32
    , sfixed64
    , float
    , double
    , enum
      -- * Strings
    , bytes
    , string
    , text
    , byteString
    , lazyByteString
      -- * Embedded Messages
    , embedded
      -- * Packed repeated fields
    , packedVarints
    , packedFixed32
    , packedFixed64
    , packedFloats
    , packedDoubles
    ) where

import           Data.Bits                     ( (.|.), shiftL, shiftR, xor )
import qualified Data.ByteString               as B
import qualified Data.ByteString.Builder       as BB
import qualified Data.ByteString.Lazy          as BL
import           Data.Int                      ( Int32, Int64 )
import           Data.Monoid                   ( (<>) )
import qualified Data.Text.Encoding            as Text.Encoding
import qualified Data.Text.Lazy                as Text.Lazy
import qualified Data.Text.Lazy.Encoding       as Text.Lazy.Encoding
import           Data.Word                     ( Word8, Word32, Word64 )
import qualified Proto3.Wire.Builder           as WB
import           Proto3.Wire.Types

-- $setup
--
-- >>> :set -XOverloadedStrings

-- | A `MessageBuilder` represents a serialized protobuf message
--
-- Use the utilities provided by this module to create `MessageBuilder`s
--
-- You can concatenate two messages using the `Monoid` instance for
-- `MessageBuilder`
--
-- Use `toLazyByteString` when you're done assembling the `MessageBuilder`
newtype MessageBuilder = MessageBuilder { unMessageBuilder :: WB.Builder }
  deriving (Monoid, Semigroup)

instance Show MessageBuilder where
  showsPrec prec builder =
      showParen (prec > 10)
        (showString "Proto3.Wire.Encode.unsafeFromLazyByteString " . shows bytes')
    where
      bytes' = toLazyByteString builder

-- | Retrieve the length of a message, in bytes
messageLength :: MessageBuilder -> Word
messageLength = WB.builderLength . unMessageBuilder

-- | Convert a message to a @"Data.ByteString.Builder".`BB.Builder`@
rawMessageBuilder :: MessageBuilder -> BB.Builder
rawMessageBuilder = WB.rawBuilder . unMessageBuilder

-- | Convert a message to a lazy `BL.ByteString`
toLazyByteString :: MessageBuilder -> BL.ByteString
toLazyByteString = WB.toLazyByteString . unMessageBuilder

-- | This lets you cast an arbitrary `ByteString` to a `MessageBuilder`, whether
-- or not the `ByteString` corresponds to a valid serialized protobuf message
--
-- Do not use this function unless you know what you're doing because it lets
-- you assemble malformed protobuf `MessageBuilder`s
unsafeFromLazyByteString :: BL.ByteString -> MessageBuilder
unsafeFromLazyByteString bytes' =
    MessageBuilder { unMessageBuilder = WB.lazyByteString bytes' }

base128Varint :: Word64 -> MessageBuilder
base128Varint i
    | i <= 0x7f = MessageBuilder (WB.word8 (fromIntegral i))
    | otherwise = MessageBuilder (WB.word8 (fromIntegral (0x80 .|. i))) <>
          base128Varint (i `shiftR` 7)

wireType :: WireType -> Word8
wireType Varint = 0
wireType Fixed32 = 5
wireType Fixed64 = 1
wireType LengthDelimited = 2

fieldHeader :: FieldNumber -> WireType -> MessageBuilder
fieldHeader num wt = base128Varint ((getFieldNumber num `shiftL` 3) .|.
                                        fromIntegral (wireType wt))

-- | Encode a 32-bit "standard" integer
--
-- For example:
--
-- >>> 1 `int32` 42
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\b*"
int32 :: FieldNumber -> Int32 -> MessageBuilder
int32 num i = fieldHeader num Varint <> base128Varint (fromIntegral i)

-- | Encode a 64-bit "standard" integer
--
-- For example:
--
-- >>> 1 `int64` (-42)
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\b\214\255\255\255\255\255\255\255\255\SOH"
int64 :: FieldNumber -> Int64 -> MessageBuilder
int64 num i = fieldHeader num Varint <> base128Varint (fromIntegral i)

-- | Encode a 32-bit unsigned integer
--
-- For example:
--
-- >>> 1 `uint32` 42
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\b*"
uint32 :: FieldNumber -> Word32 -> MessageBuilder
uint32 num i = fieldHeader num Varint <> base128Varint (fromIntegral i)

-- | Encode a 64-bit unsigned integer
--
-- For example:
--
-- >>> 1 `uint64` 42
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\b*"
uint64 :: FieldNumber -> Word64 -> MessageBuilder
uint64 num i = fieldHeader num Varint <> base128Varint (fromIntegral i)

-- | Encode a 32-bit signed integer
--
-- For example:
--
-- >>> 1 `sint32` (-42)
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\bS"
sint32 :: FieldNumber -> Int32 -> MessageBuilder
sint32 num i = int32 num ((i `shiftL` 1) `xor` (i `shiftR` 31))

-- | Encode a 64-bit signed integer
--
-- For example:
--
-- >>> 1 `sint64` (-42)
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\bS"
sint64 :: FieldNumber -> Int64 -> MessageBuilder
sint64 num i = int64 num ((i `shiftL` 1) `xor` (i `shiftR` 63))

-- | Encode a fixed-width 32-bit integer
--
-- For example:
--
-- >>> 1 `fixed32` 42
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\r*\NUL\NUL\NUL"
fixed32 :: FieldNumber -> Word32 -> MessageBuilder
fixed32 num i = fieldHeader num Fixed32 <> MessageBuilder (WB.word32LE i)

-- | Encode a fixed-width 64-bit integer
--
-- For example:
--
-- >>> 1 `fixed64` 42
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\t*\NUL\NUL\NUL\NUL\NUL\NUL\NUL"
fixed64 :: FieldNumber -> Word64 -> MessageBuilder
fixed64 num i = fieldHeader num Fixed64 <> MessageBuilder (WB.word64LE i)

-- | Encode a fixed-width signed 32-bit integer
--
-- For example:
--
-- > 1 `sfixed32` (-42)
sfixed32 :: FieldNumber -> Int32 -> MessageBuilder
sfixed32 num i = fieldHeader num Fixed32 <> MessageBuilder (WB.int32LE i)

-- | Encode a fixed-width signed 64-bit integer
--
-- For example:
--
-- >>> 1 `sfixed64` (-42)
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\t\214\255\255\255\255\255\255\255"
sfixed64 :: FieldNumber -> Int64 -> MessageBuilder
sfixed64 num i = fieldHeader num Fixed64 <> MessageBuilder (WB.int64LE i)

-- | Encode a floating point number
--
-- For example:
--
-- >>> 1 `float` 3.14
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\r\195\245H@"
float :: FieldNumber -> Float -> MessageBuilder
float num f = fieldHeader num Fixed32 <> MessageBuilder (WB.floatLE f)

-- | Encode a double-precision number
--
-- For example:
--
-- >>> 1 `double` 3.14
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\t\US\133\235Q\184\RS\t@"
double :: FieldNumber -> Double -> MessageBuilder
double num d = fieldHeader num Fixed64 <> MessageBuilder (WB.doubleLE d)

-- | Encode a value with an enumerable type.
--
-- It can be useful to derive an 'Enum' instance for a type in order to
-- emulate enums appearing in .proto files.
--
-- For example:
--
-- >>> data Shape = Circle | Square | Triangle deriving (Enum)
-- >>> 1 `enum` True <> 2 `enum` Circle
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\b\SOH\DLE\NUL"
enum :: Enum e => FieldNumber -> e -> MessageBuilder
enum num e = fieldHeader num Varint <> base128Varint (fromIntegral (fromEnum e))

-- | Encode a sequence of octets as a field of type 'bytes'.
--
-- >>> 1 `bytes` (Proto3.Wire.Builder.stringUtf8 "testing")
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\atesting"
bytes :: FieldNumber -> WB.Builder -> MessageBuilder
bytes num = embedded num . MessageBuilder

-- | Encode a UTF-8 string.
--
-- For example:
--
-- >>> 1 `string` "testing"
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\atesting"
string :: FieldNumber -> String -> MessageBuilder
string num = embedded num . MessageBuilder . WB.stringUtf8

-- | Encode lazy `Text` as UTF-8
--
-- For example:
--
-- >>> 1 `text` "testing"
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\atesting"
text :: FieldNumber -> Text.Lazy.Text -> MessageBuilder
text num txt =
    embedded num (MessageBuilder (WB.unsafeMakeBuilder len (Text.Lazy.Encoding.encodeUtf8Builder txt)))
  where
    -- It would be nice to avoid actually allocating encoded chunks,
    -- but we leave that enhancement for a future time.
    len = Text.Lazy.foldrChunks op 0 txt
    op chnk acc = fromIntegral (B.length (Text.Encoding.encodeUtf8 chnk)) + acc
{-# INLINABLE text #-}
  -- INLINABLE so that if the input is constant, the compiler
  -- has the opportunity to express its length as a CAF.

-- | Encode a collection of bytes in the form of a strict 'B.ByteString'.
--
-- For example:
--
-- >>> 1 `byteString` "testing"
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\atesting"
byteString :: FieldNumber -> B.ByteString -> MessageBuilder
byteString num bs = embedded num (MessageBuilder (WB.byteString bs))

-- | Encode a lazy bytestring.
--
-- For example:
--
-- >>> 1 `lazyByteString` "testing"
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\atesting"
lazyByteString :: FieldNumber -> BL.ByteString -> MessageBuilder
lazyByteString num bl = embedded num (MessageBuilder (WB.lazyByteString bl))

-- | Encode varints in the space-efficient packed format.
--
-- >>> 1 `packedVarints` [1, 2, 3]
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\ETX\SOH\STX\ETX"
packedVarints :: Foldable f => FieldNumber -> f Word64 -> MessageBuilder
packedVarints num = embedded num . foldMap base128Varint

-- | Encode fixed-width Word32s in the space-efficient packed format.
--
-- >>> 1 `packedFixed32` [1, 2, 3]
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\f\SOH\NUL\NUL\NUL\STX\NUL\NUL\NUL\ETX\NUL\NUL\NUL"
packedFixed32 :: Foldable f => FieldNumber -> f Word32 -> MessageBuilder
packedFixed32 num = embedded num . foldMap (MessageBuilder . WB.word32LE)

-- | Encode fixed-width Word64s in the space-efficient packed format.
--
-- >>> 1 `packedFixed64` [1, 2, 3]
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\CAN\SOH\NUL\NUL\NUL\NUL\NUL\NUL\NUL\STX\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ETX\NUL\NUL\NUL\NUL\NUL\NUL\NUL"
packedFixed64 :: Foldable f => FieldNumber -> f Word64 -> MessageBuilder
packedFixed64 num = embedded num . foldMap (MessageBuilder . WB.word64LE)

-- | Encode floats in the space-efficient packed format.
--
-- >>> 1 `packedFloats` [1, 2, 3]
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\f\NUL\NUL\128?\NUL\NUL\NUL@\NUL\NUL@@"
packedFloats :: Foldable f => FieldNumber -> f Float -> MessageBuilder
packedFloats num = embedded num . foldMap (MessageBuilder . WB.floatLE)

-- | Encode doubles in the space-efficient packed format.
--
-- >>> 1 `packedDoubles` [1, 2, 3]
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\CAN\NUL\NUL\NUL\NUL\NUL\NUL\240?\NUL\NUL\NUL\NUL\NUL\NUL\NUL@\NUL\NUL\NUL\NUL\NUL\NUL\b@"
packedDoubles :: Foldable f => FieldNumber -> f Double -> MessageBuilder
packedDoubles num = embedded num . foldMap (MessageBuilder . WB.doubleLE)

-- | Encode an embedded message.
--
-- The message is represented as a 'MessageBuilder', so it is possible to chain
-- encoding functions.
--
-- For example:
--
-- >>> 1 `embedded` (1 `string` "this message" <> 2 `string` " is embedded")
-- Proto3.Wire.Encode.unsafeFromLazyByteString "\n\FS\n\fthis message\DC2\f is embedded"
embedded :: FieldNumber -> MessageBuilder -> MessageBuilder
embedded num bb = fieldHeader num LengthDelimited <>
    base128Varint (fromIntegral (messageLength bb)) <>
    bb

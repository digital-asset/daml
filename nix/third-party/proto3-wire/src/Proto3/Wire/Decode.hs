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

-- | Low level functions for reading data in the protobufs wire format.
--
-- This module exports a function 'decodeWire' which parses data in the raw wire
-- format into an untyped 'Map' representation.
--
-- This module also provides 'Parser' types and functions for reading messages
-- from the untyped 'Map' representation obtained from 'decodeWire'.

{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}

module Proto3.Wire.Decode
    ( -- * Untyped Representation
      ParsedField(..)
    , decodeWire
      -- * Parser Types
    , Parser(..)
    , RawPrimitive
    , RawField
    , RawMessage
    , ParseError(..)
    , foldFields
    , parse
      -- * Primitives
    , bool
    , int32
    , int64
    , uint32
    , uint64
    , sint32
    , sint64
    , enum
    , byteString
    , lazyByteString
    , text
    , packedVarints
    , packedFixed32
    , packedFixed64
    , packedFloats
    , packedDoubles
    , fixed32
    , fixed64
    , sfixed32
    , sfixed64
    , float
    , double
      -- * Decoding Messages
    , at
    , oneof
    , one
    , repeated
    , embedded
    , embedded'
    ) where

import           Data.Data (Data, Typeable)
import           Control.Applicative
import           Control.Exception       ( Exception, throwIO, try )
import           Control.Monad           ( msum, foldM, ap, when )
import           Data.Bits
import qualified Data.ByteString         as B
import qualified Data.ByteString.Unsafe  as B
import qualified Data.ByteString.Lazy    as BL
import           Data.Foldable           ( foldl' )
import           Data.Hashable           ( Hashable )
import qualified Data.HashMap.Strict     as HM
import qualified Data.Map.Strict         as M
import           Data.Maybe              ( fromMaybe )
import           Data.Monoid             ( (<>) )
import           Data.Text.Lazy          ( Text, pack )
import           Data.Text.Lazy.Encoding ( decodeUtf8' )
import qualified Data.Traversable        as T
import           Data.Int                ( Int32, Int64 )
import           Data.Word               ( Word8, Word32, Word64 )
import           Proto3.Wire.Types
import qualified Safe
import           Data.IORef
import           System.IO.Unsafe (unsafePerformIO, unsafeDupablePerformIO)
import           Foreign

#if defined(__GLASGOW_HASKELL__) && !defined(__HADDOCK__)
import GHC.Base
import GHC.Word
#endif

-- | Parse a WireType. Call 'fail' if the parsed wire type is unknown.
wireType :: Word8 -> Get WireType
wireType 0 = return Varint
wireType 5 = return Fixed32
wireType 1 = return Fixed64
wireType 2 = return LengthDelimited
wireType wt = fail $ "wireType got unknown wire type: " ++ show wt

getFieldHeader :: Get (FieldNumber, WireType)
getFieldHeader = do
    word <- getBase128Varint
    wt <- wireType $ fromIntegral (word .&. 7)
    return (FieldNumber (word `shiftR` 3), wt)

-- | Decode a zigzag-encoded numeric type.
-- See: http://stackoverflow.com/questions/2210923/zig-zag-decoding
zigZagDecode :: (Num a, Bits a) => a -> a
zigZagDecode i = shiftR i 1 `xor` (-(i .&. 1))

-- | One field in a protobuf message.
--
-- We don't know what's inside some of these fields until we know what type
-- we're deserializing to, so we leave them as 'ByteString' until a later step
-- in the process.
data ParsedField = VarintField Word64
                 | Fixed32Field B.ByteString
                 | Fixed64Field B.ByteString
                 | LengthDelimitedField B.ByteString
    deriving (Show, Eq)

-- | Parse a length-delimited field.
getLengthDelimited :: Get B.ByteString
getLengthDelimited = getBase128Varint >>= (getByteString . fromIntegral)

-- | Parse a field based on its 'WireType'.
getParsedField :: WireType -> Get ParsedField
getParsedField Varint = VarintField <$> getBase128Varint
getParsedField Fixed32 =
    Fixed32Field <$> getByteString 4
getParsedField Fixed64 =
    Fixed64Field <$> getByteString 8
getParsedField LengthDelimited =
    LengthDelimitedField <$> getLengthDelimited

-- | Parse one key/value pair in a protobuf message.
getKeyVal :: Get (FieldNumber, ParsedField)
getKeyVal = do
    (fn, wt) <- getFieldHeader
    field <- getParsedField wt
    return (fn, field)

-- | Deserializes a protobuf message into a map from field number to all fields
-- labeled with that field number, in their original order.
--
-- This is necessary because of repeated fields, as well as the protobuf
-- requirement that we honor only the last element with a given field number.
--
-- This is as much structure as we can recover without knowing the type of the
-- message.
getFields :: Get [(FieldNumber, ParsedField)]
getFields = do
    keyvals <- getUntilEOF getKeyVal
    return keyvals

-- | Convert key-value pairs to a map of keys to a sequence of values with that
-- key, in their original occurrence order.
--
-- >>> toMap ([("k1", 3),("k2", 4),("k1", 6)] :: [(String,Int)])
-- fromList [("k1",fromList [3,6]),("k2",fromList [4])]
--
-- >>> toMap ([("k2", 7), ("k1", 6), ("k1", 3), ("k2", 4), ("k2", 5)] :: [(String,Int)])
-- fromList [("k1",fromList [6,3]),("k2",fromList [7,4,5])]
--
{-# INLINE toMap #-}
toMap :: (Eq k, Hashable k) => [(k, v)] -> HM.HashMap k [v]
toMap kvs0 = fmap reverse (HM.fromListWith (\[v] vs -> v : vs) kvs1)
  where
    kvs1 = map (\(k, v) -> (k, [v])) kvs0

-- | Turns a raw protobuf message into a list of 'FieldNumber' and the associated
-- 'ParsedField' value.
decodeWire :: B.ByteString
           -> Either String [(FieldNumber, ParsedField)]
decodeWire = runGet getFields

-- * Parser Interface

-- | Type describing possible errors that can be encountered while parsing.
data ParseError =
                -- | A 'WireTypeError' occurs when the type of the data in the protobuf
                -- binary format does not match the type encountered by the parser. This can
                -- indicate that the type of a field has changed or is incorrect.
                WireTypeError Text
                |
                -- | A 'BinaryError' occurs when we can't successfully parse the contents of
                -- the field.
                BinaryError Text
                |
                -- | An 'EmbeddedError' occurs when we encounter an error while parsing an
                -- embedded message.
                EmbeddedError Text
                              (Maybe ParseError)
    deriving (Show, Eq, Ord)

-- | This library does not use this instance, but it is provided for convenience,
-- so that 'ParseError' may be used with functions like `throwIO`
instance Exception ParseError

-- | A parsing function type synonym, to tidy up type signatures.
--
-- This synonym is used in three ways:
--
-- * Applied to 'RawPrimitive', to parse primitive fields.
-- * Applied to 'RawField', to parse fields which correspond to a single 'FieldNumber'.
-- * Applied to 'RawMessage', to parse entire messages.
--
-- Many of the combinators in this module are used to combine and convert between
-- these three parser types.
--
-- 'Parser's can be combined using the 'Applicative' instance.
newtype Parser input a = Parser { runParser :: input -> Either ParseError a }
    deriving Functor

instance Applicative (Parser input) where
    pure = Parser . const . pure
    {-# INLINE pure #-}
    Parser p1 <*> Parser p2 =
        Parser $ \input -> p1 input <*> p2 input
    {-# INLINE (<*>) #-}

-- | Raw data corresponding to a single encoded key/value pair.
type RawPrimitive = ParsedField

-- | Raw data corresponding to a single 'FieldNumber'.
type RawField = [RawPrimitive]

-- | Raw data corresponding to an entire message.
--
-- A 'Map' from 'FieldNumber's to the those values associated with
-- that 'FieldNumber'.
type RawMessage = HM.HashMap FieldNumber RawField

-- | Fold over a list of parsed fields accumulating a result
foldFields :: M.Map FieldNumber (Parser RawPrimitive a, a -> acc -> acc)
           -> acc
           -> [(FieldNumber, ParsedField)]
           -> Either ParseError acc
foldFields parsers = foldM applyOne
  where applyOne acc (fn, field) =
            case M.lookup fn parsers of
                Nothing              -> pure acc
                Just (parser, apply) ->
                    case runParser parser field of
                        Left err -> Left err
                        Right a  -> pure $ apply a acc

-- | Parse a message (encoded in the raw wire format) using the specified
-- `Parser`.
parse :: Parser RawMessage a -> B.ByteString -> Either ParseError a
parse parser bs = case decodeWire bs of
    Left err -> Left (BinaryError (pack err))
    Right res -> runParser parser (toMap res)

-- | To comply with the protobuf spec, if there are multiple fields with the same
-- field number, this will always return the last one.
parsedField :: RawField -> Maybe RawPrimitive
parsedField = Safe.lastMay

throwWireTypeError :: Show input
                   => String
                   -> input
                   -> Either ParseError expected
throwWireTypeError expected wrong =
    Left (WireTypeError (pack msg))
  where
    msg = "Wrong wiretype. Expected " ++ expected ++ " but got " ++ show wrong

throwCerealError :: String -> String -> Either ParseError a
throwCerealError expected cerealErr =
    Left (BinaryError (pack msg))
  where
    msg = "Failed to parse contents of " ++
        expected ++ " field. " ++ "Error from cereal was: " ++ cerealErr

parseVarInt :: Integral a => Parser RawPrimitive a
parseVarInt = Parser $
    \case
        VarintField i -> Right (fromIntegral i)
        wrong -> throwWireTypeError "varint" wrong

runGetPacked :: Get a -> Parser RawPrimitive a
runGetPacked g = Parser $
    \case
        LengthDelimitedField bs ->
            case runGet g bs of
                Left e -> throwCerealError "packed repeated field" e
                Right xs -> return xs
        wrong -> throwWireTypeError "packed repeated field" wrong

runGetFixed32 :: Get a -> Parser RawPrimitive a
runGetFixed32 g = Parser $
    \case
        Fixed32Field bs -> case runGet g bs of
            Left e -> throwCerealError "fixed32 field" e
            Right x -> return x
        wrong -> throwWireTypeError "fixed 32 field" wrong

runGetFixed64 :: Get a -> Parser RawPrimitive a
runGetFixed64 g = Parser $
    \case
        Fixed64Field bs -> case runGet g bs of
            Left e -> throwCerealError "fixed 64 field" e
            Right x -> return x
        wrong -> throwWireTypeError "fixed 64 field" wrong

bytes :: Parser RawPrimitive B.ByteString
bytes = Parser $
    \case
        LengthDelimitedField bs ->
            return bs
        wrong -> throwWireTypeError "bytes" wrong

-- | Parse a Boolean value.
bool :: Parser RawPrimitive Bool
bool = fmap (Safe.toEnumDef False) parseVarInt

-- | Parse a primitive with the @int32@ wire type.
int32 :: Parser RawPrimitive Int32
int32 = parseVarInt

-- | Parse a primitive with the @int64@ wire type.
int64 :: Parser RawPrimitive Int64
int64 = parseVarInt

-- | Parse a primitive with the @uint32@ wire type.
uint32 :: Parser RawPrimitive Word32
uint32 = parseVarInt

-- | Parse a primitive with the @uint64@ wire type.
uint64 :: Parser RawPrimitive Word64
uint64 = parseVarInt

-- | Parse a primitive with the @sint32@ wire type.
sint32 :: Parser RawPrimitive Int32
sint32 = fmap (fromIntegral . (zigZagDecode :: Word32 -> Word32)) parseVarInt

-- | Parse a primitive with the @sint64@ wire type.
sint64 :: Parser RawPrimitive Int64
sint64 = fmap (fromIntegral . (zigZagDecode :: Word64 -> Word64)) parseVarInt

-- | Parse a primitive with the @bytes@ wire type as a 'B.ByteString'.
byteString :: Parser RawPrimitive B.ByteString
byteString = bytes

-- | Parse a primitive with the @bytes@ wire type as a lazy 'BL.ByteString'.
lazyByteString :: Parser RawPrimitive BL.ByteString
lazyByteString = fmap BL.fromStrict bytes

-- | Parse a primitive with the @bytes@ wire type as 'Text'.
text :: Parser RawPrimitive Text
text = Parser $
    \case
        LengthDelimitedField bs ->
            case decodeUtf8' $ BL.fromStrict bs of
                Left err -> Left (BinaryError (pack ("Failed to decode UTF-8: " ++
                                                         show err)))
                Right txt -> return txt
        wrong -> throwWireTypeError "string" wrong

-- | Parse a primitive with an enumerated type.
--
-- This parser will return 'Left' if the encoded integer value is outside the
-- acceptable range of the 'Bounded' instance.
enum :: forall e. (Enum e, Bounded e) => Parser RawPrimitive (Either Int e)
enum = fmap toEither parseVarInt
  where
    toEither :: Int -> Either Int e
    toEither i
      | Just e <- Safe.toEnumMay i = Right e
      | otherwise = Left i

-- | Parse a packed collection of variable-width integer values (any of @int32@,
-- @int64@, @sint32@, @sint64@, @uint32@, @uint64@ or enumerations).
packedVarints :: Integral a => Parser RawPrimitive [a]
packedVarints = fmap (fmap fromIntegral) (runGetPacked (getUntilEOF getBase128Varint))

-- | Parse a packed collection of @float@ values.
packedFloats :: Parser RawPrimitive [Float]
packedFloats = runGetPacked (getUntilEOF getFloat32le)

-- | Parse a packed collection of @double@ values.
packedDoubles :: Parser RawPrimitive [Double]
packedDoubles = runGetPacked (getUntilEOF getFloat64le)

-- | Parse a packed collection of @fixed32@ values.
packedFixed32 :: Integral a => Parser RawPrimitive [a]
packedFixed32 = fmap (fmap fromIntegral) (runGetPacked (getUntilEOF getWord32le))

-- | Parse a packed collection of @fixed64@ values.
packedFixed64 :: Integral a => Parser RawPrimitive [a]
packedFixed64 = fmap (fmap fromIntegral) (runGetPacked (getUntilEOF getWord64le))

-- | Parse a @float@.
float :: Parser RawPrimitive Float
float = runGetFixed32 getFloat32le

-- | Parse a @double@.
double :: Parser RawPrimitive Double
double = runGetFixed64 getFloat64le

-- | Parse an integer primitive with the @fixed32@ wire type.
fixed32 :: Parser RawPrimitive Word32
fixed32 = runGetFixed32 getWord32le

-- | Parse an integer primitive with the @fixed64@ wire type.
fixed64 :: Parser RawPrimitive Word64
fixed64 = runGetFixed64 getWord64le

-- | Parse a signed integer primitive with the @fixed32@ wire type.
sfixed32 :: Parser RawPrimitive Int32
sfixed32 = runGetFixed32 getInt32le

-- | Parse a signed integer primitive with the @fixed64@ wire type.
sfixed64 :: Parser RawPrimitive Int64
sfixed64 = runGetFixed64 getInt64le

-- | Turn a field parser into a message parser, by specifying the 'FieldNumber'.
--
-- This parser will fail if the specified 'FieldNumber' is not present.
--
-- For example:
--
-- > one float `at` fieldNumber 1 :: Parser RawMessage (Maybe Float)
at :: Parser RawField a -> FieldNumber -> Parser RawMessage a
at parser fn = Parser $ runParser parser . fromMaybe mempty . HM.lookup fn

-- | Try to parse different field numbers with their respective parsers. This is
-- used to express alternative between possible fields of a oneof.
--
-- TODO: contrary to the protobuf spec, in the case of multiple fields number
-- matching the oneof content, the choice of field is biased to the order of the
-- list, instead of being biased to the last field of group of field number in
-- the oneof. This is related to the Map used for input that preserve order
-- across multiple invocation of the same field, but not across a group of
-- field.
oneof :: a
         -- ^ The value to produce when no field numbers belonging to the oneof
         -- are present in the input
      -> [(FieldNumber, Parser RawField a)]
         -- ^ Left-biased oneof field parsers, one per field number belonging to
         -- the oneof
      -> Parser RawMessage a
oneof def parsersByFieldNum = Parser $ \input ->
  case msum ((\(num,p) -> (p,) <$> HM.lookup num input) <$> parsersByFieldNum) of
    Nothing     -> pure def
    Just (p, v) -> runParser p v

-- | This turns a primitive parser into a field parser by keeping the
-- last received value, or return a default value if the field number is missing.
--
-- Used to ensure that we return the last value with the given field number
-- in the message, in compliance with the protobuf standard.
--
-- The protocol buffers specification specifies default values for
-- primitive types.
--
-- For example:
--
-- > one float 0 :: Parser RawField Float
{-# INLINE one #-}
one :: Parser RawPrimitive a -> a -> Parser RawField a
one parser def = Parser (fmap (fromMaybe def) . traverse (runParser parser) . parsedField)

-- | Parse a repeated field, or an unpacked collection of primitives.
--
-- Each value with the identified 'FieldNumber' will be passed to the parser
-- in the first argument, to be converted into a value of the correct type.
--
-- For example, to parse a packed collection of @uint32@ values:
--
-- > repeated uint32 :: Parser RawField [Word32]
--
-- or to parse a collection of embedded messages:
--
-- > repeated . embedded' :: Parser RawMessage a -> Parser RawField [a]
{-# INLINE repeated #-}
repeated :: Parser RawPrimitive a -> Parser RawField [a]
repeated parser = Parser (traverse (runParser parser))

-- | For a field containing an embedded message, parse as far as getting the
-- wire-level fields out of the message.
embeddedToParsedFields :: RawPrimitive -> Either ParseError RawMessage
embeddedToParsedFields (LengthDelimitedField bs) =
    case decodeWire bs of
        Left err -> Left (EmbeddedError ("Failed to parse embedded message: "
                                             <> (pack err))
                                        Nothing)
        Right result -> return (toMap result)
embeddedToParsedFields wrong =
    throwWireTypeError "embedded" wrong

-- | Create a field parser for an embedded message, from a message parser.
--
-- The protobuf spec requires that embedded messages be mergeable, so that
-- protobuf encoding has the flexibility to transmit embedded messages in
-- pieces. This function reassembles the pieces, and must be used to parse all
-- embedded non-repeated messages.
--
-- If the embedded message is not found in the outer message, this function
-- returns 'Nothing'.
{-# INLINE embedded #-}
embedded :: Parser RawMessage a -> Parser RawField (Maybe a)
embedded p = Parser $
    \xs -> if xs == empty
           then return Nothing
           else do
               innerMaps <- T.traverse embeddedToParsedFields xs
               -- TODO (FM): the concatenation field is infrequent but
               -- currently quadratic -- fix it eventually...
               let combinedMap = foldl' (HM.unionWith (<>)) mempty innerMaps
               parsed <- runParser p combinedMap
               return $ Just parsed

-- | Create a primitive parser for an embedded message from a message parser.
--
-- This parser does no merging of fields if multiple message fragments are
-- sent separately.
{-# INLINE embedded' #-}
embedded' :: Parser RawMessage a -> Parser RawPrimitive a
embedded' parser = Parser $
    \case
        LengthDelimitedField bs ->
            case parse parser bs of
                Left err -> Left (EmbeddedError "Failed to parse embedded message."
                                                (Just err))
                Right result -> return result
        wrong -> throwWireTypeError "embedded" wrong

------------------------------------------------------------------------
-- Unchecked shifts

shiftl_w32 :: Word32 -> Int -> Word32
shiftl_w64 :: Word64 -> Int -> Word64

#if defined(__GLASGOW_HASKELL__) && !defined(__HADDOCK__)
shiftl_w32 (W32# w) (I# i) = W32# (w `uncheckedShiftL#`   i)

#if WORD_SIZE_IN_BITS < 64
shiftl_w64 (W64# w) (I# i) = W64# (w `uncheckedShiftL64#` i)

#if __GLASGOW_HASKELL__ <= 606
-- Exported by GHC.Word in GHC 6.8 and higher
foreign import ccall unsafe "stg_uncheckedShiftL64"
    uncheckedShiftL64#     :: Word64# -> Int# -> Word64#
#endif

#else
shiftl_w64 (W64# w) (I# i) = W64# (w `uncheckedShiftL#` i)
#endif

#else
shiftl_w16 = shiftL
shiftl_w32 = shiftL
shiftl_w64 = shiftL
#endif

-----------------------------------------------------------------------
-- Small Get toolkit

newtype GetException = GetException String
  deriving (Data, Typeable, Show, Eq)
instance Exception GetException

newtype Get a = Get {unGet :: IORef B.ByteString -> IO a}

{-# NOINLINE runGet #-}
runGet :: Get a -> B.ByteString -> Either String a
runGet (Get f) bs = let
  mbErr = unsafePerformIO $ try $ do
    bsRef <- newIORef bs
    f bsRef
  in case mbErr of
    Left (GetException err) -> Left err
    Right x -> Right x

instance Functor Get where
  {-# INLINE fmap #-}
  fmap f (Get g) = Get (\ref -> fmap f (g ref))

instance Applicative Get where
  {-# INLINE pure #-}
  pure = return
  {-# INLINE (<*>) #-}
  (<*>) = ap

instance Monad Get where
  {-# INLINE return #-}
  return x = Get (\_ -> return x)
  {-# INLINE (>>=) #-}
  Get f >>= g = Get $ \ref -> do
    x <- f ref
    unGet (g x) ref
  {-# INLINE fail #-}
  fail x = Get (\_ -> throwIO (GetException x))

{-# INLINE getWord8 #-}
getWord8 :: Get Word8
getWord8 = Get $ \bsRef -> do
  bs <- readIORef bsRef
  case B.uncons bs of
    Nothing -> fail "getWord8: no bytes"
    Just (w, bs') -> do
      writeIORef bsRef bs'
      return w

{-# INLINE getByteString #-}
getByteString :: Int -> Get B.ByteString
getByteString n = Get $ \bsRef -> do
  bs <- readIORef bsRef
  when (B.length bs < n) $
    fail "getByteString: not enough bytes"
  let (bs1, bs2) = B.splitAt n bs
  writeIORef bsRef bs2
  return bs1

{-# INLINE isEmpty #-}
isEmpty :: Get Bool
isEmpty = Get $ \bsRef -> do
  bs <- readIORef bsRef
  return (B.null bs)

-- | Get a base128 varint. Handles delimitation by MSB.
{-# INLINE getBase128Varint #-}
getBase128Varint :: Get Word64
getBase128Varint = loop 0 0
  where
    loop !i !w64 = do
        w8 <- getWord8
        if base128Terminal w8
            then return $ combine i w64 w8
            else loop (i + 1) (combine i w64 w8)
    base128Terminal w8 = (not . (`testBit` 7)) $ w8
    combine i w64 w8 = (w64 .|.
                            (fromIntegral (w8 `clearBit` 7)
                             `shiftL`
                             (i * 7)))

{-# INLINE getUntilEOF #-}
getUntilEOF :: Get a -> Get [a]
getUntilEOF p = go
  where
    go = do
      end <- isEmpty
      if end
        then return []
        else do
          x <- p
          (x :) <$> go

getFloat32le :: Get Float
getFloat32le = wordToFloat <$> getWord32le

getFloat64le :: Get Double
getFloat64le = wordToDouble <$> getWord64le

getWord32le :: Get Word32
getWord32le = do
  s <- getByteString 4
  return $! (fromIntegral (s `B.unsafeIndex` 3) `shiftl_w32` 24) .|.
            (fromIntegral (s `B.unsafeIndex` 2) `shiftl_w32` 16) .|.
            (fromIntegral (s `B.unsafeIndex` 1) `shiftl_w32`  8) .|.
            (fromIntegral (s `B.unsafeIndex` 0) )

getWord64le :: Get Word64
getWord64le = do
  s <- getByteString 8
  return $! (fromIntegral (s `B.unsafeIndex` 7) `shiftl_w64` 56) .|.
            (fromIntegral (s `B.unsafeIndex` 6) `shiftl_w64` 48) .|.
            (fromIntegral (s `B.unsafeIndex` 5) `shiftl_w64` 40) .|.
            (fromIntegral (s `B.unsafeIndex` 4) `shiftl_w64` 32) .|.
            (fromIntegral (s `B.unsafeIndex` 3) `shiftl_w64` 24) .|.
            (fromIntegral (s `B.unsafeIndex` 2) `shiftl_w64` 16) .|.
            (fromIntegral (s `B.unsafeIndex` 1) `shiftl_w64`  8) .|.
            (fromIntegral (s `B.unsafeIndex` 0) )

getInt32le :: Get Int32
getInt32le = do
  s <- getByteString 4
  return $! (fromIntegral (s `B.unsafeIndex` 3) `shiftL` 24) .|.
            (fromIntegral (s `B.unsafeIndex` 2) `shiftL` 16) .|.
            (fromIntegral (s `B.unsafeIndex` 1) `shiftL`  8) .|.
            (fromIntegral (s `B.unsafeIndex` 0) )

getInt64le :: Get Int64
getInt64le = do
  s <- getByteString 8
  return $! (fromIntegral (s `B.unsafeIndex` 7) `shiftL` 56) .|.
            (fromIntegral (s `B.unsafeIndex` 6) `shiftL` 48) .|.
            (fromIntegral (s `B.unsafeIndex` 5) `shiftL` 40) .|.
            (fromIntegral (s `B.unsafeIndex` 4) `shiftL` 32) .|.
            (fromIntegral (s `B.unsafeIndex` 3) `shiftL` 24) .|.
            (fromIntegral (s `B.unsafeIndex` 2) `shiftL` 16) .|.
            (fromIntegral (s `B.unsafeIndex` 1) `shiftL`  8) .|.
            (fromIntegral (s `B.unsafeIndex` 0) )

{-# INLINE getInt32le #-}
{-# INLINE getInt64le #-}
{-# INLINE getFloat32le #-}
{-# INLINE getFloat64le #-}
{-# INLINE getWord32le #-}
{-# INLINE getWord64le #-}

{-# INLINE wordToFloat #-}
wordToFloat :: Word32 -> Float
wordToFloat w = unsafeDupablePerformIO $ alloca $ \(ptr :: Ptr Word32) -> do
  poke ptr w
  peek (castPtr ptr)

{-# INLINE wordToDouble #-}
wordToDouble :: Word64 -> Double
wordToDouble w = unsafeDupablePerformIO $ alloca $ \(ptr :: Ptr Word64) -> do
  poke ptr w
  peek (castPtr ptr)


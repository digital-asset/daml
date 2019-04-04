{-# LANGUAGE DefaultSignatures   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}

-- | Support for the "JSONPB" canonical JSON encoding described at
-- https://developers.google.com/protocol-buffers/docs/proto3#json.
--
-- This modules provides 'Data.Aeson'-like helper functions, typeclasses, and
-- instances for converting to and from values of types which have a JSONPB
-- representation and equivalent underlying 'Data.Aeson' representations.
--
-- This module also presents a (very minimal) surface syntax for Aeson-like
-- operations; the idea is that we can write 'ToJSONPB' and 'FromJSONPB'
-- instances in a very similar manner to 'A.ToJSON' and 'A.FromJSON' instances,
-- except that doing so specifies JSONPB codecs instead of vanilla JSON codecs.
--
-- Example use:
--
-- @
-- message Scalar32 {
--   int32     i32 = 1;
--   uint32    u32 = 2;
--   sint32    s32 = 3;
--   fixed32   f32 = 4;
--   sfixed32 sf32 = 5;
-- }
--
-- instance ToJSONPB Scalar32 where
--   toJSONPB (Scalar32 i32 u32 s32 f32 sf32) = object
--       [ "i32"  .= i32
--       , "u32"  .= u32
--       , "s32"  .= s32
--       , "f32"  .= f32
--       , "sf32" .= sf32
--       ]
--   toEncodingPB (Scalar32 i32 u32 s32 f32 sf32) = pairs
--       [ "i32"  .= i32
--       , "u32"  .= u32
--       , "s32"  .= s32
--       , "f32"  .= f32
--       , "sf32" .= sf32
--       ]
--
-- instance FromJSONPB Scalar32 where
--   parseJSONPB = withObject "Scalar32" $ \obj ->
--     pure Scalar32
--     <*> obj .: "i32"
--     <*> obj .: "u32"
--     <*> obj .: "s32"
--     <*> obj .: "f32"
--     <*> obj .: "sf32"
-- @

module Proto3.Suite.JSONPB.Class where

import qualified Data.Aeson                       as A (Encoding, FromJSON (..),
                                                        FromJSONKey (..),
                                                        FromJSONKeyFunction (..),
                                                        ToJSON (..), Value (..),
                                                        eitherDecode, json,
                                                        (.!=))
import qualified Data.Aeson.Encoding              as E
import qualified Data.Aeson.Internal              as A (formatError, iparse)
import qualified Data.Aeson.Parser                as A (eitherDecodeWith)
import qualified Data.Aeson.Types                 as A (Object, Pair, Parser,
                                                        Series,
                                                        explicitParseField,
                                                        explicitParseFieldMaybe,
                                                        object, typeMismatch)
import qualified Data.Attoparsec.ByteString       as Atto (skipWhile)
import qualified Data.Attoparsec.ByteString.Char8 as Atto (Parser, endOfInput)
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Base64           as B64
import qualified Data.ByteString.Lazy             as LBS
import           Data.Coerce
import           Data.Proxy
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import           GHC.Int                          (Int32, Int64)
import           GHC.Word                         (Word32, Word64)
import           Proto3.Suite.Class               (HasDefault (def, isDefault),
                                                   Named (nameOf))
import           Proto3.Suite.Types               (Enumerated (..), Fixed (..))

-- * Typeclass definitions

-- | 'A.ToJSON' variant for JSONPB direct encoding via 'A.Encoding'
class ToJSONPB a where
  -- | 'A.toJSON' variant for JSONPB encoders.
  toJSONPB :: a -> Options -> A.Value

  -- | 'A.toEncoding' variant for JSONPB encoders. If an implementation is not
  -- provided, uses 'toJSONPB' (which is less efficient since it indirects
  -- through the 'A.Value' IR).
  toEncodingPB :: a -> Options -> A.Encoding
  toEncodingPB x = A.toEncoding . toJSONPB x

-- | 'A.FromJSON' variant for JSONPB decoding from the 'A.Value' IR
class FromJSONPB a where
  -- | 'A.parseJSON' variant for JSONPB decoders.
  parseJSONPB :: A.Value -> A.Parser a

-- * JSONPB codec entry points

-- | 'Data.Aeson.encode' variant for serializing a JSONPB value as a lazy
-- 'LBS.ByteString'.
encode :: ToJSONPB a => Options -> a -> LBS.ByteString
encode opts x = E.encodingToLazyByteString (toEncodingPB x opts)
{-# INLINE encode #-}

-- | 'Data.Aeson.eitherDecode' variant for deserializing a JSONPB value from a
-- lazy 'LBS.ByteString'.
eitherDecode :: FromJSONPB a => LBS.ByteString -> Either String a
eitherDecode = eitherFormatError . A.eitherDecodeWith jsonEOF (A.iparse parseJSONPB)
  where
    eitherFormatError = either (Left . uncurry A.formatError) Right
    {-# INLINE eitherFormatError #-}

    -- NB: cribbed from aeson-1.1.1.0:Data.Aeson.Parser.Internal.jsonEOF, which
    -- is not exported. It's simple, so we just inline it here. Might be worth
    -- submitting a PR to export this.
    jsonEOF :: Atto.Parser A.Value
    jsonEOF = A.json <* skipSpace <* Atto.endOfInput
      where
        skipSpace :: Atto.Parser ()
        skipSpace = Atto.skipWhile $ \w -> w == 0x20 || w == 0x0a || w == 0x0d || w == 0x09
        {-# INLINE skipSpace #-}
{-# INLINE eitherDecode #-}

-- * Operator definitions

-- | JSONPB-encoded monoidal key-value pairs
class Monoid m => KeyValuePB m where
  pair :: ToJSONPB v => Text -> v -> Options -> m

instance KeyValuePB A.Series where pair k v opts = E.pair k (toEncodingPB v opts)
instance KeyValuePB [A.Pair] where pair k v opts = pure (k, toJSONPB v opts)

-- | Construct a monoidal key-value pair, using 'mempty' to represent omission
-- of default values (unless the given 'Options' force their emission).
(.=) :: (HasDefault v, ToJSONPB v, KeyValuePB kvp) => Text -> v -> Options -> kvp
k .= v = mk
  where
    mk opts@Options{..}
      | not optEmitDefaultValuedFields && isDefault v
        = mempty
      | otherwise
        = pair k v opts

-- | 'Data.Aeson..:' variant for JSONPB; if the given key is missing from the
-- object, or if it is present but its value is null, we produce the default
-- protobuf value for the field type
(.:) :: (FromJSONPB a, HasDefault a) => A.Object -> Text -> A.Parser a
obj .: key = obj .:? key A..!= def
  where
    (.:?) = A.explicitParseFieldMaybe parseJSONPB

parseField :: FromJSONPB a
           => A.Object -> Text -> A.Parser a
parseField = A.explicitParseField parseJSONPB

-- * JSONPB rendering and parsing options

data Options = Options
  { optEmitDefaultValuedFields :: Bool
  } deriving Show

-- | Default options for JSONPB encoding. By default, all options are @False@.
defaultOptions :: Options
defaultOptions = Options
  { optEmitDefaultValuedFields = False
  }

-- * Helper types and functions

dropNamedPrefix :: Named a => Proxy a -> String -> String
dropNamedPrefix p = drop (length (nameOf p :: String))

object :: [Options -> [A.Pair]] -> Options -> A.Value
object fs = A.object . mconcat fs

pairs :: [Options -> A.Series] -> Options -> E.Encoding
pairs fs = E.pairs . mconcat fs

enumFieldString :: forall a. (Named a, Show a) => a -> A.Value
enumFieldString = A.String . T.pack . dropNamedPrefix (Proxy @a) . show

enumFieldEncoding :: forall a. (Named a, Show a) => a -> A.Encoding
enumFieldEncoding = E.string . dropNamedPrefix (Proxy @a) . show

-- | A 'Data.Aeson' 'A.Value' encoder for values which can be
-- JSONPB-encoded
toAesonValue :: ToJSONPB a => a -> A.Value
toAesonValue = flip toJSONPB defaultOptions

-- | A direct 'A.Encoding' for values which can be JSONPB-encoded
toAesonEncoding :: ToJSONPB a => a -> A.Encoding
toAesonEncoding = flip toEncodingPB defaultOptions

-- | Parse a JSONPB floating point value; first parameter provides context for
-- type mismatches
parseFP :: (A.FromJSON a, A.FromJSONKey a) => String -> A.Value -> A.Parser a
parseFP tyDesc v = case v of
  A.Number{} -> A.parseJSON v
  A.String t -> case A.fromJSONKey of
                  A.FromJSONKeyTextParser p
                    -> p t
                  _ -> fail "internal: parseKeyPB: unexpected FromJSONKey summand"
  _          -> A.typeMismatch tyDesc v

-- | Liberally parse an integer value (e.g. 42 or "42" as 42); first parameter
-- provides context for type mismatches
parseNumOrDecimalString :: (A.FromJSON a) => String -> A.Value -> A.Parser a
parseNumOrDecimalString tyDesc v = case v of
  A.Number{} -> A.parseJSON v
  A.String t -> either fail pure . A.eitherDecode . TL.encodeUtf8 . TL.fromStrict $ t
  _          -> A.typeMismatch tyDesc v

-- * Common instances for jsonpb codec implementations

-- ** Instances for scalar types

--------------------------------------------------------------------------------
-- Boolean scalar type

instance ToJSONPB Bool where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding

instance FromJSONPB Bool where
  parseJSONPB = A.parseJSON

--------------------------------------------------------------------------------
-- Integer scalar types
--
--   * 32 bit integer values render to JSON decimal numbers; either numbers or
--     strings are accepted.
--
--   * 64 bit integer values render to JSON decimal strings; either numbers
--     or strings are accepted.
--

-- int32 / sint32
instance ToJSONPB Int32 where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding

instance FromJSONPB Int32 where
  parseJSONPB = parseNumOrDecimalString "int32 / sint32"

-- uint32
instance ToJSONPB Word32 where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding

instance FromJSONPB Word32 where
  parseJSONPB = parseNumOrDecimalString "uint32"

-- int64 / sint64
instance ToJSONPB Int64 where
  toJSONPB x _     = A.String . T.pack . show $ x
  toEncodingPB x _ = E.string (show x)
instance FromJSONPB Int64 where
  parseJSONPB = parseNumOrDecimalString "int64 / sint64"

-- unit64
instance ToJSONPB Word64 where
  toJSONPB x _     = A.String . T.pack . show $ x
  toEncodingPB x _ = E.string (show x)
instance FromJSONPB Word64 where
  parseJSONPB = parseNumOrDecimalString "int64 / sint64"

-- fixed32
instance ToJSONPB (Fixed Word32) where
  toJSONPB     = coerce (toJSONPB @Word32)
  toEncodingPB = coerce (toEncodingPB @Word32)
instance FromJSONPB (Fixed Word32) where
  parseJSONPB = coerce (parseJSONPB @Word32)

-- fixed64
instance ToJSONPB (Fixed Word64) where
  toJSONPB = coerce (toJSONPB @Word64)
  toEncodingPB = coerce (toEncodingPB @Word64)
instance FromJSONPB (Fixed Word64) where
  parseJSONPB = coerce (parseJSONPB @Word64)

-- sfixed32
instance ToJSONPB (Fixed Int32) where
  toJSONPB = coerce (toJSONPB @Int32)
  toEncodingPB = coerce (toEncodingPB @Int32)
instance FromJSONPB (Fixed Int32) where
  parseJSONPB = coerce (parseJSONPB @Int32)

-- sfixed64
instance ToJSONPB (Fixed Int64) where
  toJSONPB = coerce (toJSONPB @Int64)
  toEncodingPB = coerce (toEncodingPB @Int64)
instance FromJSONPB (Fixed Int64) where
  parseJSONPB = coerce (parseJSONPB @Int64)

--------------------------------------------------------------------------------
-- Floating point scalar types
--
-- JSON value will be a number or one of the special string values "NaN",
-- "Infinity", and "-Infinity". Either numbers or strings are accepted. Exponent
-- notation is also accepted.

-- float
instance ToJSONPB Float where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding

instance FromJSONPB Float where
  parseJSONPB = parseFP "float"

-- double
instance ToJSONPB Double where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding
instance FromJSONPB Double where
  parseJSONPB = parseFP "double"

--------------------------------------------------------------------------------
-- Stringly types (string and bytes)

-- string
instance ToJSONPB TL.Text where
  toJSONPB     = const . A.toJSON
  toEncodingPB = const . A.toEncoding
instance FromJSONPB TL.Text where
  parseJSONPB = A.parseJSON

-- bytes

bsToJSONPB :: BS.ByteString -> A.Value
bsToJSONPB (T.decodeUtf8' . B64.encode -> ebs) = case ebs of
  Right bs -> A.toJSON bs
  Left e   -> error ("internal: failed to encode B64-encoded bytestring: " ++ show e)
              -- NB: T.decodeUtf8' should never fail because we B64-encode the
              -- incoming bytestring.

instance ToJSONPB BS.ByteString where
  toJSONPB bs _        = bsToJSONPB bs
  toEncodingPB bs opts = E.value (toJSONPB bs opts)

instance FromJSONPB BS.ByteString where
  parseJSONPB (A.String b64enc) = pure . B64.decodeLenient . T.encodeUtf8 $ b64enc
  parseJSONPB v                 = A.typeMismatch "bytes" v

--------------------------------------------------------------------------------
-- Enumerated types

enumToJSONPB :: (e -> Options -> a) -- ^ JSONPB encoder function to use
             -> a                   -- ^ null value to use for out-of-range enums
             -> Enumerated e        -- ^ the enumerated value to encode
             -> Options             -- ^ JSONPB encoding options
             -> a                   -- ^ the JSONPB-encoded value
enumToJSONPB enc null_ (Enumerated e) opts = either err (`enc` opts) e
  where
    err 0 = error "enumToJSONPB: The first enum value must be zero in proto3"
            -- TODO: Raise a compilation error when the first enum value in an
            -- enumeration is not zero.
            --
            -- See https://github.com/awakesecurity/proto3-suite/issues/28
            --
            -- The proto3 spec states that the default value is the first
            -- defined enum value, which must be 0. Since we currently don't
            -- raise a compilation error for this like we should, we have to
            -- handle this case.
            --
            -- For now, die horribly to mimic what should be a compilation
            -- error.
    err _ = null_
            -- From the JSONPB spec:
            --
            --   If a value is missing in the JSON-encoded data or if its value
            --   is null, it will be interpreted as the appropriate default
            --   value when parsed into a protocol buffer.
            --
            -- Thus, interpreting a wire value out of enum range as "missing",
            -- we yield null here to mean the default value.


instance ToJSONPB e => ToJSONPB (Enumerated e) where
  toJSONPB     = enumToJSONPB toJSONPB A.Null
  toEncodingPB = enumToJSONPB toEncodingPB E.null_

instance (Bounded e, Enum e, FromJSONPB e) => FromJSONPB (Enumerated e) where
  parseJSONPB A.Null = pure def -- So CG does not have to handle this case in
                                -- every generated instance
  parseJSONPB v      = Enumerated . Right <$> parseJSONPB v

-- ** Instances for composite types

--------------------------------------------------------------------------------
-- Instances for repeated messages
--
-- JSON value will be the vector elements encoded as a JSON array. The null
-- value is accepted as the empty list, @[]@.

instance ToJSONPB a => ToJSONPB (V.Vector a) where
  toJSONPB v opts     = A.Array (V.map (`toJSONPB`  opts) v)
  toEncodingPB v opts = E.list (`toEncodingPB` opts) (V.toList v)
instance FromJSONPB a => FromJSONPB (V.Vector a) where
  parseJSONPB (A.Array vs) = mapM parseJSONPB vs
  parseJSONPB A.Null       = pure []
  parseJSONPB v            = A.typeMismatch "repeated" v

--------------------------------------------------------------------------------
-- Instances for nested messages

instance ToJSONPB a => ToJSONPB (Maybe a) where
  toJSONPB mx opts     = maybe A.Null (`toJSONPB` opts) mx
  toEncodingPB mx opts = maybe E.null_ (`toEncodingPB` opts) mx
instance FromJSONPB a => FromJSONPB (Maybe a) where
  parseJSONPB A.Null = pure Nothing
  parseJSONPB v      = fmap Just (parseJSONPB v)

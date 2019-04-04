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
-- | This module is an executable tutorial for the @proto3-wire@ library.
-- It will demonstrate how to encode and decode messages of various types.
--
-- = Imports
--
-- We recommend importing the "Proto3.Wire.Encode" and "Proto3.Wire.Decode"
-- modules qualified, since they define encoding and decoding functions with the
-- same names.
--
-- The "Proto3.Wire" module reexports some useful functions, so a good default
-- set of imports is:
--
-- > import           Proto3.Wire
-- > import qualified Proto3.Wire.Encode as Encode
-- > import qualified Proto3.Wire.Decode as Decode
--
-- = Primitives
--
-- Let's translate this simple @.proto@ file into a Haskell data type and a pair
-- of encoding and decoding functions:
--
-- > message EchoRequest {
-- >   string message = 1;
-- > }
--
-- We begin by defining a data type to represent our messages:
--
-- > data EchoRequest = EchoRequest { echoRequestMessage :: Text }
--
-- == Encoding
--
-- To encode an 'EchoRequest', we use the @Encode.'Encode.text'@ function, and provide
-- the field number and the text value:
--
-- > encodeEchoRequest :: EchoRequest -> Encode.MessageBuilder
-- > encodeEchoRequest EchoRequest{..} =
-- >     Encode.text 1 echoRequestMessage
--
-- Fields of type @string@ can be encoded\/decoded from\/to values of type 'String',
-- 'ByteString' and 'Text'. Here we use the 'Text' type, which is encoded using
-- the 'Encode.text' function. Different primitive types have different encoding
-- functions, which are usually named after the Protocol Buffers type.
--
-- == Decoding
--
-- To decode an 'EchoRequest', we use the 'Decode.parse' function, and provide
-- a 'Decode.Parser' to extract the fields:
--
-- > decodeEchoRequest :: ByteString -> Either Decode.ParseError EchoRequest
-- > decodeEchoRequest = Decode.parse echoRequestParser
--
-- The decoding function for 'Text' is called @Decode.'Decode.text'@. However, we must
-- specify the field number, which is done using the `at` function, and provide
-- a default value, using the `one` function. The types will ensure that
-- the field number and default value are provided.
--
-- We use the 'Functor' instance for 'Decode.Parser' to apply the 'EchoRequest'
-- constructor to the result:
--
-- > echoRequestParser :: Decode.Parser Decode.RawMessage EchoRequest
-- > echoRequestParser = EchoRequest <$> (one Decode.text mempty `at` 1
--
-- = Messages with multiple fields
--
-- Let's make our example more interesting by including multiple fields:
--
-- > message EchoResponse {
-- >   string message = 1;
-- >   uint64 timestamp = 2;
-- > }
--
-- We begin by defining a data type to represent our messages:
--
-- > data EchoResponse = EchoResponse { echoResponseMessage   :: Text
-- >                                  , echoResponseTimestamp :: Word64
-- >                                  }
--
-- == Encoding
--
-- To encode messages with multiple fields, note that functions in the
-- "Proto3.Wire.Encode" module return values in the 'Encode.MessageBuilder'
-- monoid, so we can use `mappend` to combine messages:
--
-- > encodedEchoResponse :: EchoResponse -> Encode.MessageBuilder
-- > encodedEchoResponse EchoResponse{..} =
-- >     Encode.text 1 echoResponseMessage <>
-- >         Encode.uint64 2 echoResponseTimestamp
--
-- However, be careful to always use increasing field numbers, since this is not
-- enforced by the library.
--
-- == Decoding
--
-- Messages with many fields can be parsed using the 'Applicative' instance for
-- 'Parser':
--
-- > decodeEchoResponse :: ByteString -> Either Decode.ParseError EchoResponse
-- > decodeEchoResponse = Decode.parse echoResponseParser
-- >
-- > echoResponseParser :: Decode.Parser Decode.RawMessage EchoResponse
-- > echoResponseParser = EchoResponse <$> (one Decode.text mempty `at` 1)
-- >                                   <*> (one Decode.uint64 0 `at` 2)
--
-- = Repeated Fields and Embedded Messages
--
-- Messages can be embedded in fields of other messages. This can be useful
-- when entire sections of a message can be repeated or omitted.
--
-- Consider the following message types:
--
-- > message EchoManyRequest {
-- >   repeated EchoRequest requests = 1;
-- > }
--
-- Again, we define a type corresponding to our message:
--
-- > data EchoManyRequest = EchoManyRequest { echoManyRequestRequests :: Seq EchoRequest }
--
-- == Encoding
--
-- Messages can be embedded using `Encode.embedded`.
--
-- In protocol buffers version 3, all fields are optional. To omit a value for a
-- field, simply do not append it to the 'Encode.MessageBuilder'.
--
-- Similarly, repeated fields can be encoded by concatenating several values
-- with the same 'FieldNumber'.
--
-- It can be useful to use 'foldMap' to deal with these cases.
--
-- > encodeEchoManyRequest :: EchoManyRequest -> Encode.MessageBuilder
-- > encodeEchoManyRequest =
-- >   foldMap (Encode.embedded 1 . encodeEchoRequest)
-- >   . echoManyRequestRequests
--
-- == Decoding
--
-- Embedded messages can be decoded using 'Decode.embedded'.
--
-- Repeated fields can be decoded using 'repeated'.
--
-- Repeated embedded messages can be decoded using @repeated . Decode.embedded'@.
--
-- > decodeEchoManyRequest :: ByteString -> Either Decode.ParseError EchoManyRequest
-- > decodeEchoManyRequest = Decode.parse echoManyRequestParser
-- >
-- > echoManyRequestParser :: Decode.Parser Decode.RawMessage EchoManyRequest
-- > echoManyRequestParser =
-- >   EchoManyRequest <$> (repeated (Decode.embedded' echoRequestParser) `at` 1)
{-# LANGUAGE RecordWildCards #-}

module Proto3.Wire.Tutorial where

import           Data.ByteString         ( ByteString )
import           Data.Monoid             ( (<>) )
import           Data.Text.Lazy          ( Text )
import           Data.Word               ( Word64 )

import           Proto3.Wire
import qualified Proto3.Wire.Encode      as Encode
import qualified Proto3.Wire.Decode      as Decode

data EchoRequest = EchoRequest { echoRequestMessage :: Text }

encodeEchoRequest :: EchoRequest -> Encode.MessageBuilder
encodeEchoRequest EchoRequest{..} =
    Encode.text 1 echoRequestMessage

decodeEchoRequest :: ByteString -> Either Decode.ParseError EchoRequest
decodeEchoRequest = Decode.parse echoRequestParser

echoRequestParser :: Decode.Parser Decode.RawMessage EchoRequest
echoRequestParser = EchoRequest <$> (one Decode.text mempty `at` 1)

data EchoResponse = EchoResponse { echoResponseMessage   :: Text
                                 , echoResponseTimestamp :: Word64
                                 }

encodedEchoResponse :: EchoResponse -> Encode.MessageBuilder
encodedEchoResponse EchoResponse{..} =
    Encode.text 1 echoResponseMessage <>
        Encode.uint64 2 echoResponseTimestamp

decodeEchoResponse :: ByteString -> Either Decode.ParseError EchoResponse
decodeEchoResponse = Decode.parse echoResponseParser

echoResponseParser :: Decode.Parser Decode.RawMessage EchoResponse
echoResponseParser = EchoResponse <$> (one Decode.text mempty `at` 1)
                                  <*> (one Decode.uint64 0 `at` 2)

newtype EchoManyRequest = EchoManyRequest { echoManyRequestRequests :: [EchoRequest] }

encodeEchoManyRequest :: EchoManyRequest -> Encode.MessageBuilder
encodeEchoManyRequest = foldMap (Encode.embedded 1 .
                                     encodeEchoRequest) .
    echoManyRequestRequests

decodeEchoManyRequest :: ByteString -> Either Decode.ParseError EchoManyRequest
decodeEchoManyRequest = Decode.parse echoManyRequestParser

echoManyRequestParser :: Decode.Parser Decode.RawMessage EchoManyRequest
echoManyRequestParser = EchoManyRequest <$> (repeated (Decode.embedded' echoRequestParser) `at` 1)

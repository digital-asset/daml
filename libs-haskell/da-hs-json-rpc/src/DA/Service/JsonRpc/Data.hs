-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
-- | Implementation of basic JSON-RPC data types.
module DA.Service.JsonRpc.Data
( -- * Requests
  Request(..)
, BatchRequest(..)
  -- ** Parsing
, FromRequest(..)
, fromRequest
  -- ** Encoding
, ToRequest(..)
, buildRequest

  -- * Responses
, Response(..)
, BatchResponse(..)
  -- ** Parsing
, FromResponse(..)
, fromResponse
  -- ** Encoding
, Respond
, buildResponse
  -- ** Errors
, ErrorObj(..)
, fromError
  -- ** Error messages
, errorParse
, errorInvalid
, errorParams
, errorMethod
, errorId

  -- * Others
, Message(..)
, Method
, Id(..)
, fromId
, Ver(..)

) where

import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import Control.DeepSeq
import Control.Monad
import Data.Aeson (encode)
import Data.Aeson.Types
import Data.Hashable (Hashable)
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding
import GHC.Generics (Generic)


--
-- Requests
--

data Request = Request { getReqVer      :: !Ver
                       , getReqMethod   :: !Method
                       , getReqParams   :: !Value
                       , getReqId       :: !Id
                       }
             | Notif   { getReqVer      :: !Ver
                       , getReqMethod   :: !Method
                       , getReqParams   :: !Value
                       }
             deriving (Eq, Show, Generic)

instance NFData Request where
    rnf (Request v m p i) = rnf v `seq` rnf m `seq` rnf p `seq` rnf i
    rnf (Notif v m p) = rnf v `seq` rnf m `seq` rnf p

instance ToJSON Request where
    toJSON (Request V2 m p i) = object $ case p of
        Null -> [jr2, "method" .= m, "id" .= i]
        _    -> [jr2, "method" .= m, "id" .= i, "params" .= p]
    toJSON (Request V1 m p i) = object $ case p of
        Null -> ["method" .= m, "params" .= emptyArray, "id" .= i]
        _    -> ["method" .= m, "params" .= p, "id" .= i]
    toJSON (Notif V2 m p) = object $ case p of
        Null -> [jr2, "method" .= m]
        _    -> [jr2, "method" .= m, "params" .= p]
    toJSON (Notif V1 m p) = object $ case p of
        Null -> ["method" .= m, "params" .= emptyArray, "id" .= Null]
        _    -> ["method" .= m, "params" .= p, "id" .= Null]

class FromRequest q where
    -- | Parser for params Value in JSON-RPC request.
    parseParams :: Method -> Maybe (Value -> Parser q)

fromRequest :: FromRequest q => Request -> Either ErrorObj q
fromRequest req =
    case parserM of
        Nothing -> Left $ errorMethod m
        Just parser ->
            case parseMaybe parser p of
                Nothing -> Left $ errorParams p
                Just  q -> Right q
  where
    m = getReqMethod req
    p = getReqParams req
    parserM = parseParams m

instance FromRequest Value where
    parseParams = const $ Just return

instance FromRequest () where
    parseParams = const . Just . const $ return ()

instance FromJSON Request where
    parseJSON = withObject "request" $ \o -> do
        (v, n, m, p) <- parseVerIdMethParams o
        case n of Nothing -> return $ Notif   v m p
                  Just i  -> return $ Request v m p i

parseVerIdMethParams :: Object -> Parser (Ver, Maybe Id, Method, Value)
parseVerIdMethParams o = do
    v <- parseVer o
    i <- o .:? "id"
    m <- o .: "method"
    p <- o .:? "params" .!= Null
    return (v, i, m, p)

class ToRequest q where
    -- | Method associated with request data to build a request object.
    requestMethod :: q -> Method

    -- | Is this request to be sent as a notification (no id, no response)?
    requestIsNotif :: q -> Bool

instance ToRequest Value where
    requestMethod = const "json"
    requestIsNotif = const False

instance ToRequest () where
    requestMethod = const "json"
    requestIsNotif = const False

buildRequest :: (ToJSON q, ToRequest q)
             => Ver             -- ^ JSON-RPC version
             -> q               -- ^ Request data
             -> Id
             -> Request
buildRequest ver q = if requestIsNotif q
                         then const $ Notif ver (requestMethod q) (toJSON q)
                         else Request ver (requestMethod q) (toJSON q)

--
-- Responses
--

data Response = Response      { getResVer :: !Ver
                              , getResult :: !Value
                              , getResId  :: !Id
                              }
              | ResponseError { getResVer :: !Ver
                              , getError  :: !ErrorObj
                              , getResId  :: !Id
                              }
              | OrphanError   { getResVer :: !Ver
                              , getError  :: !ErrorObj
                              }
              deriving (Eq, Show, Generic)


instance NFData Response where
    rnf (Response v r i) = rnf v `seq` rnf r `seq` rnf i
    rnf (ResponseError v o i) = rnf v `seq` rnf o `seq` rnf i
    rnf (OrphanError v o) = rnf v `seq` rnf o

instance ToJSON Response where
    toJSON (Response V1 r i) = object
        ["id" .= i, "result" .= r, "error" .= Null]
    toJSON (Response V2 r i) = object
        [jr2, "id" .= i, "result" .= r]
    toJSON (ResponseError V1 e i) = object
        ["id" .= i, "error" .= e, "result" .= Null]
    toJSON (ResponseError V2 e i) = object
        [jr2, "id" .= i, "error" .= e]
    toJSON (OrphanError V1 e) = object
        ["id" .= Null, "error" .= e, "result" .= Null]
    toJSON (OrphanError V2 e) = object
        [jr2, "id" .= Null, "error" .= e]

class FromResponse r where
    -- | Parser for result Value in JSON-RPC response.
    -- Method corresponds to request to which this response answers.
    parseResult :: Method -> Maybe (Value -> Parser r)

-- | Parse a response knowing the method of the corresponding request.
fromResponse :: FromResponse r => Method -> Response -> Maybe r
fromResponse m (Response _ r _) = parseResult m >>= flip parseMaybe r
fromResponse _ _ = Nothing

instance FromResponse Value where
    parseResult = const $ Just return

instance FromResponse () where
    parseResult = const Nothing

instance FromJSON Response where
    parseJSON = withObject "response" $ \o -> do
        (v, d, s) <- parseVerIdResultError o
        case s of
            Right r -> do
                guard $ isJust d
                return $ Response v r (fromJust d)
            Left e ->
                case d of
                    Just  i -> return $ ResponseError v e i
                    Nothing -> return $ OrphanError v e

parseVerIdResultError :: Object
                      -> Parser (Ver, Maybe Id, Either ErrorObj Value)
parseVerIdResultError o = do
    v <- parseVer o
    i <- o .:? "id"
    r <- o .:? "result" .!= Null
    p <- if r == Null then Left <$> o .: "error" else return $ Right r
    return (v, i, p)

-- | Create a response from a request. Use in servers.
buildResponse :: (Monad m, FromRequest q, ToJSON r)
              => Respond q m r
              -> Request
              -> m (Maybe Response)
buildResponse f req@(Request v _ _ i) =
    case fromRequest req of
        Left e -> return . Just $ ResponseError v e i
        Right q -> do
            rE <- f q
            case rE of
                Left  e -> return . Just $ ResponseError v e i
                Right r -> return . Just $ Response v (toJSON r) i
buildResponse _ _ = return Nothing

-- | Type of function to make it easy to create a response from a request.
-- Meant to be used in servers.
type Respond q m r = q -> m (Either ErrorObj r)

-- | Error object from JSON-RPC 2.0. ErrorVal for backwards compatibility.
data ErrorObj = ErrorObj  { getErrMsg  :: !String
                          , getErrCode :: !Int
                          , getErrData :: !Value
                          }
              | ErrorVal  { getErrData :: !Value }
              deriving (Show, Eq, Generic)

instance NFData ErrorObj where
    rnf (ErrorObj m c d) = rnf m `seq` rnf c `seq` rnf d
    rnf (ErrorVal v) = rnf v

instance FromJSON ErrorObj where
    parseJSON Null = mzero
    parseJSON v@(Object o) = p1 <|> p2 where
        p1 = do
            m <- o .: "message"
            c <- o .: "code"
            d <- o .:? "data" .!= Null
            return $ ErrorObj m c d
        p2 = return $ ErrorVal v
    parseJSON v = return $ ErrorVal v

instance ToJSON ErrorObj where
    toJSON (ErrorObj s i d) = object $ ["message" .= s, "code" .= i]
        ++ if d == Null then [] else ["data" .= d]
    toJSON (ErrorVal v) = v

-- | Get a user-friendly string with the error information.
fromError :: ErrorObj -> String
fromError (ErrorObj m c v) = show c ++ ": " ++ m ++ ": " ++ valueAsString v
fromError (ErrorVal (String t)) = T.unpack t
fromError (ErrorVal v) = valueAsString v

valueAsString :: Value -> String
valueAsString = T.unpack . decodeUtf8 . L.toStrict . encode

-- | Parse error.
errorParse :: ByteString -> ErrorObj
errorParse = ErrorObj "Parse error" (-32700) . String . decodeUtf8

-- | Invalid request.
errorInvalid :: Value -> ErrorObj
errorInvalid = ErrorObj "Invalid request" (-32600)

-- | Invalid params.
errorParams :: Value -> ErrorObj
errorParams = ErrorObj "Invalid params" (-32602)

-- | Method not found.
errorMethod :: Method -> ErrorObj
errorMethod = ErrorObj "Method not found" (-32601) . toJSON

-- | Id not recognized.
errorId :: Id -> ErrorObj
errorId = ErrorObj "Id not recognized" (-32000) . toJSON


--
-- Messages
--

data BatchRequest
    = BatchRequest     { getBatchRequest  :: ![Request] }
    | SingleRequest    { getSingleRequest ::  !Request  }
    deriving (Eq, Show, Generic)

instance NFData BatchRequest where
    rnf (BatchRequest qs) = rnf qs
    rnf (SingleRequest q) = rnf q

instance FromJSON BatchRequest where
    parseJSON qs@Array{} = BatchRequest  <$> parseJSON qs
    parseJSON q@Object{} = SingleRequest <$> parseJSON q
    parseJSON _ = mzero

instance ToJSON BatchRequest where
    toJSON (BatchRequest qs) = toJSON qs
    toJSON (SingleRequest q) = toJSON q

data BatchResponse
    = BatchResponse    { getBatchResponse :: ![Response] }
    | SingleResponse   { getSingleResponse :: !Response  }
    deriving (Eq, Show, Generic)

instance NFData BatchResponse where
    rnf (BatchResponse qs) = rnf qs
    rnf (SingleResponse q) = rnf q

instance FromJSON BatchResponse where
    parseJSON qs@Array{} = BatchResponse  <$> parseJSON qs
    parseJSON q@Object{} = SingleResponse <$> parseJSON q
    parseJSON _ = mzero

instance ToJSON BatchResponse where
    toJSON (BatchResponse qs) = toJSON qs
    toJSON (SingleResponse q) = toJSON q

data Message
    = MsgRequest  { getMsgRequest  :: !Request   }
    | MsgResponse { getMsgResponse :: !Response  }
    | MsgBatch    { getBatch       :: ![Message] }
    deriving (Eq, Show, Generic)

instance NFData Message where
    rnf (MsgRequest  q) = rnf q
    rnf (MsgResponse r) = rnf r
    rnf (MsgBatch    b) = rnf b

instance ToJSON Message where
    toJSON (MsgRequest  q) = toJSON q
    toJSON (MsgResponse r) = toJSON r
    toJSON (MsgBatch    b) = toJSON b

instance FromJSON Message where
    parseJSON v = (MsgRequest   <$> parseJSON v)
              <|> (MsgResponse  <$> parseJSON v)
              <|> (MsgBatch     <$> parseJSON v)

--
-- Types
--

type Method = Text

data Id = IdInt { getIdInt :: !Int  }
        | IdTxt { getIdTxt :: !Text }
    deriving (Eq, Ord, Show, Read, Generic)

instance Hashable Id

instance NFData Id where
    rnf (IdInt i) = rnf i
    rnf (IdTxt t) = rnf t

instance Enum Id where
    toEnum = IdInt
    fromEnum (IdInt i) = i
    fromEnum _ = error "Can't enumerate non-integral ids"

instance FromJSON Id where
    parseJSON s@(String _) = IdTxt <$> parseJSON s
    parseJSON n@(Number _) = IdInt <$> parseJSON n
    parseJSON _ = mzero

instance ToJSON Id where
    toJSON (IdTxt s) = toJSON s
    toJSON (IdInt n) = toJSON n

-- | Pretty display a message id. Meant for logs.
fromId :: Id -> String
fromId (IdInt i) = show i
fromId (IdTxt t) = T.unpack t

-- | JSON-RPC version.
data Ver = V1 -- ^ JSON-RPC 1.0
         | V2 -- ^ JSON-RPC 2.0
         deriving (Eq, Show, Read, Generic)

instance NFData Ver where
    rnf v = v `seq` ()

jr2 :: Pair
jr2 = "jsonrpc" .= ("2.0" :: Text)

parseVer :: Object -> Parser Ver
parseVer o = do
    j <- o .:? "jsonrpc"
    return $ if j == Just ("2.0" :: Text) then V2 else V1

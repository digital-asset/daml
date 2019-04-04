{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE KindSignatures    #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}

module Network.GRPC.HighLevel.Server where

import qualified Control.Exception                         as CE
import           Control.Monad
import           Data.ByteString                           (ByteString)
import qualified Data.ByteString.Lazy                      as BL
import           Network.GRPC.LowLevel
import           Proto3.Suite.Class
import           System.IO

type ServerCallMetadata = ServerCall ()

type ServiceServer service = service ServerRequest ServerResponse

data ServerRequest (streamType :: GRPCMethodType) request response where
  ServerNormalRequest :: ServerCallMetadata -> request -> ServerRequest 'Normal request response
  ServerReaderRequest :: ServerCallMetadata -> StreamRecv request -> ServerRequest 'ClientStreaming request response
  ServerWriterRequest :: ServerCallMetadata -> request -> StreamSend response -> ServerRequest 'ServerStreaming request response
  ServerBiDiRequest :: ServerCallMetadata -> StreamRecv request -> StreamSend response -> ServerRequest 'BiDiStreaming request response

data ServerResponse (streamType :: GRPCMethodType) response where
  ServerNormalResponse :: response -> MetadataMap -> StatusCode -> StatusDetails
                       -> ServerResponse 'Normal response
  ServerReaderResponse :: Maybe response -> MetadataMap -> StatusCode -> StatusDetails
                       -> ServerResponse 'ClientStreaming response
  ServerWriterResponse :: MetadataMap -> StatusCode -> StatusDetails
                       -> ServerResponse 'ServerStreaming response
  ServerBiDiResponse :: MetadataMap -> StatusCode -> StatusDetails
                     -> ServerResponse 'BiDiStreaming response

type ServerHandler a b =
  ServerCall a
  -> IO (b, MetadataMap, StatusCode, StatusDetails)

convertGeneratedServerHandler ::
  (ServerRequest 'Normal request response -> IO (ServerResponse 'Normal response))
  -> ServerHandler request response
convertGeneratedServerHandler handler call =
  do let call' = call { payload = () }
     ServerNormalResponse rsp meta stsCode stsDetails <-
       handler (ServerNormalRequest call' (payload call))
     return (rsp, meta, stsCode, stsDetails)

convertServerHandler :: (Message a, Message b)
                     => ServerHandler a b
                     -> ServerHandlerLL
convertServerHandler f c = case fromByteString (payload c) of
  Left x  -> CE.throw (GRPCIODecodeError $ show x)
  Right x -> do (y, tm, sc, sd) <- f (fmap (const x) c)
                return (toBS y, tm, sc, sd)

type ServerReaderHandler a b
  =  ServerCall (MethodPayload 'ClientStreaming)
  -> StreamRecv a
  -> IO (Maybe b, MetadataMap, StatusCode, StatusDetails)

convertGeneratedServerReaderHandler ::
  (ServerRequest 'ClientStreaming request response -> IO (ServerResponse 'ClientStreaming response))
  -> ServerReaderHandler request response
convertGeneratedServerReaderHandler handler call recv =
  do ServerReaderResponse rsp meta stsCode stsDetails <-
       handler (ServerReaderRequest call recv)
     return (rsp, meta, stsCode, stsDetails)

convertServerReaderHandler :: (Message a, Message b)
                           => ServerReaderHandler a b
                           -> ServerReaderHandlerLL
convertServerReaderHandler f c recv =
  serialize <$> f c (convertRecv recv)
  where
    serialize (mmsg, m, sc, sd) = (toBS <$> mmsg, m, sc, sd)

type ServerWriterHandler a b =
     ServerCall a
  -> StreamSend b
  -> IO (MetadataMap, StatusCode, StatusDetails)

convertGeneratedServerWriterHandler ::
  (ServerRequest 'ServerStreaming request response -> IO (ServerResponse 'ServerStreaming response))
  -> ServerWriterHandler request response
convertGeneratedServerWriterHandler handler call send =
  do let call' = call { payload = () }
     ServerWriterResponse meta stsCode stsDetails <-
       handler (ServerWriterRequest call' (payload call) send)
     return (meta, stsCode, stsDetails)

convertServerWriterHandler :: (Message a, Message b) =>
                              ServerWriterHandler a b
                              -> ServerWriterHandlerLL
convertServerWriterHandler f c send =
  f (convert <$> c) (convertSend send)
  where
    convert bs = case fromByteString bs of
      Left x  -> CE.throw (GRPCIODecodeError $ show x)
      Right x -> x

type ServerRWHandler a b
  =  ServerCall (MethodPayload 'BiDiStreaming)
  -> StreamRecv a
  -> StreamSend b
  -> IO (MetadataMap, StatusCode, StatusDetails)

convertGeneratedServerRWHandler ::
  (ServerRequest 'BiDiStreaming request response -> IO (ServerResponse 'BiDiStreaming response))
  -> ServerRWHandler request response
convertGeneratedServerRWHandler handler call recv send =
  do ServerBiDiResponse meta stsCode stsDetails <-
       handler (ServerBiDiRequest call recv send)
     return (meta, stsCode, stsDetails)

convertServerRWHandler :: (Message a, Message b)
                       => ServerRWHandler a b
                       -> ServerRWHandlerLL
convertServerRWHandler f c recv send =
  f c (convertRecv recv) (convertSend send)

convertRecv :: Message a => StreamRecv ByteString -> StreamRecv a
convertRecv =
  fmap $ \e -> do
    msg <- e
    case msg of
      Nothing -> return Nothing
      Just bs -> case fromByteString bs of
                   Left x  -> Left (GRPCIODecodeError $ show x)
                   Right x -> return (Just x)

convertSend :: Message a => StreamSend ByteString -> StreamSend a
convertSend s = s . toBS

toBS :: Message a => a -> ByteString
toBS = BL.toStrict . toLazyByteString

data Handler (a :: GRPCMethodType) where
  UnaryHandler        :: (Message c, Message d) => MethodName -> ServerHandler c d       -> Handler 'Normal
  ClientStreamHandler :: (Message c, Message d) => MethodName -> ServerReaderHandler c d -> Handler 'ClientStreaming
  ServerStreamHandler :: (Message c, Message d) => MethodName -> ServerWriterHandler c d -> Handler 'ServerStreaming
  BiDiStreamHandler   :: (Message c, Message d) => MethodName -> ServerRWHandler c d     -> Handler 'BiDiStreaming

data AnyHandler = forall (a :: GRPCMethodType). AnyHandler (Handler a)

anyHandlerMethodName :: AnyHandler -> MethodName
anyHandlerMethodName (AnyHandler m) = handlerMethodName m

handlerMethodName :: Handler a -> MethodName
handlerMethodName (UnaryHandler m _)        = m
handlerMethodName (ClientStreamHandler m _) = m
handlerMethodName (ServerStreamHandler m _) = m
handlerMethodName (BiDiStreamHandler m _)   = m

-- | Handles errors that result from trying to handle a call on the server.
-- For each error, takes a different action depending on the severity in the
-- context of handling a server call. This also tries to give an indication of
-- whether the error is our fault or user error.
handleCallError :: (String -> IO ())
                   -- ^ logging function
                   -> Either GRPCIOError a
                   -> IO ()
handleCallError _ (Right _) = return ()
handleCallError _ (Left GRPCIOTimeout) =
  -- Probably a benign timeout (such as a client disappearing), noop for now.
  return ()
handleCallError _ (Left GRPCIOShutdown) =
  -- Server shutting down. Benign.
  return ()
handleCallError logMsg (Left (GRPCIODecodeError e)) =
  logMsg $ "Decoding error: " ++ show e
handleCallError logMsg (Left (GRPCIOHandlerException e)) =
  logMsg $ "Handler exception caught: " ++ show e
handleCallError logMsg (Left x) =
  logMsg $ show x ++ ": This probably indicates a bug in gRPC-haskell. Please report this error."

loopWError :: Int
           -> ServerOptions
           -> IO (Either GRPCIOError a)
           -> IO ()
loopWError i o@ServerOptions{..} f = do
   when (i `mod` 100 == 0) $ putStrLn $ "i = " ++ show i
   f >>= handleCallError optLogger
   loopWError (i + 1) o f

-- TODO: options for setting initial/trailing metadata
handleLoop :: Server
           -> ServerOptions
           -> (Handler a, RegisteredMethod a)
           -> IO ()
handleLoop s o (UnaryHandler _ f, rm) =
  loopWError 0 o $ serverHandleNormalCall s rm mempty $ convertServerHandler f
handleLoop s o (ClientStreamHandler _ f, rm) =
  loopWError 0 o $ serverReader s rm mempty $ convertServerReaderHandler f
handleLoop s o (ServerStreamHandler _ f, rm) =
  loopWError 0 o $ serverWriter s rm mempty $ convertServerWriterHandler f
handleLoop s o (BiDiStreamHandler _ f, rm) =
  loopWError 0 o $ serverRW s rm mempty $ convertServerRWHandler f

data ServerOptions = ServerOptions
  { optNormalHandlers       :: [Handler 'Normal]
    -- ^ Handlers for unary (non-streaming) calls.
  , optClientStreamHandlers :: [Handler 'ClientStreaming]
    -- ^ Handlers for client streaming calls.
  , optServerStreamHandlers :: [Handler 'ServerStreaming]
    -- ^ Handlers for server streaming calls.
  , optBiDiStreamHandlers   :: [Handler 'BiDiStreaming]
    -- ^ Handlers for bidirectional streaming calls.
  , optServerHost           :: Host
    -- ^ Name of the host the server is running on.
  , optServerPort           :: Port
    -- ^ Port on which to listen for requests.
  , optUseCompression       :: Bool
    -- ^ Whether to use compression when communicating with the client.
  , optUserAgentPrefix      :: String
    -- ^ Optional custom prefix to add to the user agent string.
  , optUserAgentSuffix      :: String
    -- ^ Optional custom suffix to add to the user agent string.
  , optInitialMetadata      :: MetadataMap
    -- ^ Metadata to send at the beginning of each call.
  , optSSLConfig            :: Maybe ServerSSLConfig
    -- ^ Security configuration.
  , optLogger               :: String -> IO ()
    -- ^ Logging function to use to log errors in handling calls.
  }

defaultOptions :: ServerOptions
defaultOptions = ServerOptions
  { optNormalHandlers       = []
  , optClientStreamHandlers = []
  , optServerStreamHandlers = []
  , optBiDiStreamHandlers   = []
  , optServerHost           = "localhost"
  , optServerPort           = 50051
  , optUseCompression       = False
  , optUserAgentPrefix      = "grpc-haskell/0.0.0"
  , optUserAgentSuffix      = ""
  , optInitialMetadata      = mempty
  , optSSLConfig            = Nothing
  , optLogger               = hPutStrLn stderr
  }

serverLoop :: ServerOptions -> IO ()
serverLoop _opts = fail "Registered method-based serverLoop NYI"
{-
  withGRPC $ \grpc ->
    withServer grpc (mkConfig opts) $ \server -> do
      let rmsN = zip (optNormalHandlers opts) $ normalMethods server
      let rmsCS = zip (optClientStreamHandlers opts) $ cstreamingMethods server
      let rmsSS = zip (optServerStreamHandlers opts) $ sstreamingMethods server
      let rmsB = zip (optBiDiStreamHandlers opts) $ bidiStreamingMethods server
      --TODO: Perhaps assert that no methods disappeared after registration.
      let loop :: forall a. (Handler a, RegisteredMethod a) -> IO ()
          loop = handleLoop server
      asyncsN <- mapM async $ map loop rmsN
      asyncsCS <- mapM async $ map loop rmsCS
      asyncsSS <- mapM async $ map loop rmsSS
      asyncsB <- mapM async $ map loop rmsB
      asyncUnk <- async $ loopWError 0 $ unknownHandler server
      waitAnyCancel $ asyncUnk : asyncsN ++ asyncsCS ++ asyncsSS ++ asyncsB
      return ()
  where
    mkConfig ServerOptions{..} =
      ServerConfig
        {  host = "localhost"
         , port = optServerPort
         , methodsToRegisterNormal = map handlerMethodName optNormalHandlers
         , methodsToRegisterClientStreaming =
             map handlerMethodName optClientStreamHandlers
         , methodsToRegisterServerStreaming =
             map handlerMethodName optServerStreamHandlers
         , methodsToRegisterBiDiStreaming =
             map handlerMethodName optBiDiStreamHandlers
         , serverArgs =
             ([CompressionAlgArg GrpcCompressDeflate | optUseCompression]
              ++
              [UserAgentPrefix optUserAgentPrefix
               , UserAgentSuffix optUserAgentSuffix])
        }
    unknownHandler s =
      --TODO: is this working?
      U.serverHandleNormalCall s mempty $ \call _ -> do
        logMsg $ "Requested unknown endpoint: " ++ show (U.callMethod call)
        return ("", mempty, StatusNotFound,
                StatusDetails "Unknown method")
-}

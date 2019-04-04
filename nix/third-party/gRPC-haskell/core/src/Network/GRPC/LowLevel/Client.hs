{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE ViewPatterns      #-}

-- | This module defines data structures and operations pertaining to registered
-- clients using registered calls; for unregistered support, see
-- `Network.GRPC.LowLevel.Client.Unregistered`.
module Network.GRPC.LowLevel.Client where

import           Control.Exception                     (bracket, finally)
import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except
import qualified Data.ByteString                       as B
import           Data.ByteString                       (ByteString)
import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.CompletionQueue
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op
import qualified Network.GRPC.Unsafe                   as C
import qualified Network.GRPC.Unsafe.ChannelArgs       as C
import qualified Network.GRPC.Unsafe.Constants         as C
import qualified Network.GRPC.Unsafe.Op                as C
import qualified Network.GRPC.Unsafe.Security          as C
import qualified Network.GRPC.Unsafe.Time              as C

-- | Represents the context needed to perform client-side gRPC operations.
data Client = Client {clientChannel :: C.Channel,
                      clientCQ      :: CompletionQueue,
                      clientConfig  :: ClientConfig
                     }

data ClientSSLKeyCertPair = ClientSSLKeyCertPair
  {clientPrivateKey :: FilePath,
   clientCert :: FilePath}

-- | SSL configuration for the client. It's perfectly acceptable for both fields
-- to be 'Nothing', in which case default fallbacks will be used for the server
-- root cert.
data ClientSSLConfig = ClientSSLConfig
  {serverRootCert :: Maybe FilePath,
   -- ^ Path to the server root certificate. If 'Nothing', gRPC will attempt to
   -- fall back to a default.
   clientSSLKeyCertPair :: Maybe ClientSSLKeyCertPair,
   -- ^ The client's private key and cert, if available.
   clientMetadataPlugin :: Maybe C.ClientMetadataCreate
   -- ^ Optional plugin for attaching additional metadata to each call.
  }

-- | Configuration necessary to set up a client.

data ClientConfig = ClientConfig {clientServerHost :: Host,
                                  clientServerPort :: Port,
                                  clientArgs :: [C.Arg],
                                  -- ^ Optional arguments for setting up the
                                  -- channel on the client. Supplying an empty
                                  -- list will cause the channel to use gRPC's
                                  -- default options.
                                  clientSSLConfig :: Maybe ClientSSLConfig
                                  -- ^ If 'Nothing', the client will use an
                                  -- insecure connection to the server.
                                  -- Otherwise, will use the supplied config to
                                  -- connect using SSL.
                                 }

clientEndpoint :: ClientConfig -> Endpoint
clientEndpoint ClientConfig{..} = endpoint clientServerHost clientServerPort

addMetadataCreds :: C.ChannelCredentials
                    -> Maybe C.ClientMetadataCreate
                    -> IO C.ChannelCredentials
addMetadataCreds c Nothing = return c
addMetadataCreds c (Just create) = do
  callCreds <- C.createCustomCallCredentials create
  C.compositeChannelCredentialsCreate c callCreds C.reserved

createChannel :: ClientConfig -> C.GrpcChannelArgs -> IO C.Channel
createChannel conf@ClientConfig{..} chanargs =
  case clientSSLConfig of
    Nothing -> C.grpcInsecureChannelCreate e chanargs C.reserved
    Just (ClientSSLConfig rootCertPath Nothing plugin) ->
      do rootCert <- mapM B.readFile rootCertPath
         C.withChannelCredentials rootCert Nothing Nothing $ \creds -> do
           creds' <- addMetadataCreds creds plugin
           C.secureChannelCreate creds' e chanargs C.reserved
    Just (ClientSSLConfig x (Just (ClientSSLKeyCertPair y z)) plugin) ->
      do rootCert <- mapM B.readFile x
         privKey <- Just <$> B.readFile y
         clientCert <- Just <$> B.readFile z
         C.withChannelCredentials rootCert privKey clientCert $ \creds -> do
           creds' <- addMetadataCreds creds plugin
           C.secureChannelCreate creds' e chanargs C.reserved
  where (Endpoint e) = clientEndpoint conf

createClient :: GRPC -> ClientConfig -> IO Client
createClient grpc clientConfig =
  C.withChannelArgs (clientArgs clientConfig) $ \chanargs -> do
    clientChannel <- createChannel clientConfig chanargs
    clientCQ <- createCompletionQueue grpc
    return Client{..}

destroyClient :: Client -> IO ()
destroyClient Client{..} = do
  grpcDebug "destroyClient: calling grpc_channel_destroy()"
  C.grpcChannelDestroy clientChannel
  grpcDebug "destroyClient: shutting down CQ."
  shutdownResult <- shutdownCompletionQueue clientCQ
  case shutdownResult of
    Left x -> do putStrLn $ "Failed to stop client CQ: " ++ show x
                 putStrLn $ "Trying to shut down anyway."
    Right _ -> return ()

withClient :: GRPC -> ClientConfig -> (Client -> IO a) -> IO a
withClient grpc config = bracket (createClient grpc config)
                                 (\c -> grpcDebug "withClient: destroying."
                                        >> destroyClient c)

clientConnectivity :: Client -> IO C.ConnectivityState
clientConnectivity Client{..} =
  C.grpcChannelCheckConnectivityState clientChannel False

--TODO: We should probably also register client methods on startup.

-- | Register a method on the client so that we can call it with
-- 'clientRequest'.
clientRegisterMethod :: Client
                     -> MethodName
                     -> IO (C.CallHandle)
clientRegisterMethod Client{..} meth = do
  let e = clientEndpoint clientConfig
  C.grpcChannelRegisterCall clientChannel
                            (unMethodName meth)
                            (unEndpoint e)
                            C.reserved


clientRegisterMethodNormal :: Client
                           -> MethodName
                           -> IO (RegisteredMethod 'Normal)
clientRegisterMethodNormal c meth = do
  let e = clientEndpoint (clientConfig c)
  h <- clientRegisterMethod c meth
  return $ RegisteredMethodNormal meth e h


clientRegisterMethodClientStreaming :: Client
                                    -> MethodName
                                    -> IO (RegisteredMethod 'ClientStreaming)
clientRegisterMethodClientStreaming c meth = do
  let e = clientEndpoint (clientConfig c)
  h <- clientRegisterMethod c meth
  return $  RegisteredMethodClientStreaming meth e h

clientRegisterMethodServerStreaming :: Client
                                    -> MethodName
                                    -> IO (RegisteredMethod 'ServerStreaming)
clientRegisterMethodServerStreaming c meth = do
  let e = clientEndpoint (clientConfig c)
  h <- clientRegisterMethod c meth
  return $ RegisteredMethodServerStreaming meth e h


clientRegisterMethodBiDiStreaming :: Client
                                  -> MethodName
                                  -> IO (RegisteredMethod 'BiDiStreaming)
clientRegisterMethodBiDiStreaming c meth = do
  let e = clientEndpoint (clientConfig c)
  h <- clientRegisterMethod c meth
  return $ RegisteredMethodBiDiStreaming meth e h

-- | Create a new call on the client for a registered method.
-- Returns 'Left' if the CQ is shutting down or if the job to create a call
-- timed out.
clientCreateCall :: Client
                 -> RegisteredMethod mt
                 -> TimeoutSeconds
                 -> IO (Either GRPCIOError ClientCall)
clientCreateCall c rm ts = clientCreateCallParent c rm ts Nothing

-- | For servers that act as clients to other gRPC servers, this version creates
-- a client call with an optional parent server call. This allows for cascading
-- call cancellation from the `ServerCall` to the `ClientCall`.
clientCreateCallParent :: Client
                           -> RegisteredMethod mt
                           -> TimeoutSeconds
                           -> Maybe (ServerCall a)
                           -- ^ Optional parent call for cascading cancellation.
                           -> IO (Either GRPCIOError ClientCall)
clientCreateCallParent Client{..} rm timeout parent = do
  C.withDeadlineSeconds timeout $ \deadline -> do
    channelCreateCall clientChannel parent C.propagateDefaults
      clientCQ (methodHandle rm) deadline

-- | Handles safe creation and cleanup of a client call
withClientCall :: Client
               -> RegisteredMethod mt
               -> TimeoutSeconds
               -> (ClientCall -> IO (Either GRPCIOError a))
               -> IO (Either GRPCIOError a)
withClientCall cl rm tm = withClientCallParent cl rm tm Nothing

-- | Handles safe creation and cleanup of a client call, with an optional parent
-- call parameter. This allows for cancellation to cascade from the parent
-- `ServerCall` to the created `ClientCall`. Obviously, this is only useful if
-- the given gRPC client is also a server.
withClientCallParent :: Client
                     -> RegisteredMethod mt
                     -> TimeoutSeconds
                     -> Maybe (ServerCall b)
                        -- ^ Optional parent call for cascading cancellation
                     -> (ClientCall -> IO (Either GRPCIOError a))
                     -> IO (Either GRPCIOError a)
withClientCallParent cl rm tm parent f =
  clientCreateCallParent cl rm tm parent >>= \case
    Left e  -> return (Left e)
    Right c -> f c `finally` do
      debugClientCall c
      grpcDebug "withClientCall(R): destroying."
      destroyClientCall c

data NormalRequestResult = NormalRequestResult
  { rspBody :: ByteString
  , initMD  :: MetadataMap -- ^ initial metadata
  , trailMD :: MetadataMap -- ^ trailing metadata
  , rspCode :: C.StatusCode
  , details :: StatusDetails
  }
  deriving (Show, Eq)

-- | Function for assembling call result when the 'MethodType' is 'Normal'.
compileNormalRequestResults :: [OpRecvResult]
                               -> Either GRPCIOError NormalRequestResult
compileNormalRequestResults
  [OpRecvInitialMetadataResult m,
   OpRecvMessageResult (Just body),
   OpRecvStatusOnClientResult m2 status details]
    = Right $ NormalRequestResult body m m2 status (StatusDetails details)
compileNormalRequestResults x =
  case extractStatusInfo x of
    Nothing -> Left GRPCIOUnknownError
    Just (_meta, status, details) ->
      Left (GRPCIOBadStatusCode status (StatusDetails details))

--------------------------------------------------------------------------------
-- clientReader (client side of server streaming mode)

-- | First parameter is initial server metadata.
type ClientReaderHandler = MetadataMap -> StreamRecv ByteString -> IO ()
type ClientReaderResult  = (MetadataMap, C.StatusCode, StatusDetails)

clientReader :: Client
             -> RegisteredMethod 'ServerStreaming
             -> TimeoutSeconds
             -> ByteString -- ^ The body of the request
             -> MetadataMap -- ^ Metadata to send with the request
             -> ClientReaderHandler
             -> IO (Either GRPCIOError ClientReaderResult)
clientReader cl@Client{ clientCQ = cq } rm tm body initMeta f =
  withClientCall cl rm tm go
  where
    go (unsafeCC -> c) = runExceptT $ do
      void $ runOps' c cq [ OpSendInitialMetadata initMeta
                          , OpSendMessage body
                          , OpSendCloseFromClient
                          ]
      srvMD <- recvInitialMetadata c cq
      liftIO $ f srvMD (streamRecvPrim c cq)
      recvStatusOnClient c cq

--------------------------------------------------------------------------------
-- clientWriter (client side of client streaming mode)

type ClientWriterHandler = StreamSend ByteString -> IO ()
type ClientWriterResult  = (Maybe ByteString, MetadataMap, MetadataMap,
                              C.StatusCode, StatusDetails)

clientWriter :: Client
             -> RegisteredMethod 'ClientStreaming
             -> TimeoutSeconds
             -> MetadataMap -- ^ Initial client metadata
             -> ClientWriterHandler
             -> IO (Either GRPCIOError ClientWriterResult)
clientWriter cl rm tm initMeta =
  withClientCall cl rm tm . clientWriterCmn cl initMeta

clientWriterCmn :: Client -- ^ The active client
                -> MetadataMap -- ^ Initial client metadata
                -> ClientWriterHandler
                -> ClientCall -- ^ The active client call
                -> IO (Either GRPCIOError ClientWriterResult)
clientWriterCmn (clientCQ -> cq) initMeta f (unsafeCC -> c) =
  runExceptT $ do
    sendInitialMetadata c cq initMeta
    liftIO $ f (streamSendPrim c cq)
    sendSingle c cq OpSendCloseFromClient
    let ops = [OpRecvInitialMetadata, OpRecvMessage, OpRecvStatusOnClient]
    runOps' c cq ops >>= \case
      CWRFinal mmsg initMD trailMD st ds
        -> return (mmsg, initMD, trailMD, st, ds)
      _ -> throwE (GRPCIOInternalUnexpectedRecv "clientWriter")

pattern CWRFinal :: Maybe ByteString
                 -> MetadataMap
                 -> MetadataMap
                 -> C.StatusCode
                 -> StatusDetails
                 -> [OpRecvResult]
pattern CWRFinal mmsg initMD trailMD st ds
  <- [ OpRecvInitialMetadataResult initMD
     , OpRecvMessageResult mmsg
     , OpRecvStatusOnClientResult trailMD st (StatusDetails -> ds)
     ]

--------------------------------------------------------------------------------
-- clientRW (client side of bidirectional streaming mode)

type ClientRWHandler
  =  IO (Either GRPCIOError MetadataMap)
  -> StreamRecv ByteString
  -> StreamSend ByteString
  -> WritesDone
  -> IO ()
type ClientRWResult = (MetadataMap, C.StatusCode, StatusDetails)

clientRW :: Client
         -> RegisteredMethod 'BiDiStreaming
         -> TimeoutSeconds
         -> MetadataMap
         -> ClientRWHandler
         -> IO (Either GRPCIOError ClientRWResult)
clientRW cl rm tm initMeta f =
  withClientCall cl rm tm (\cc -> clientRW' cl cc initMeta f)

-- | The most generic version of clientRW. It does not assume anything about
-- threading model; caller must invoke the WritesDone operation, exactly once,
-- for the half-close, after all threads have completed writing. TODO: It'd be
-- nice to find a way to type-enforce this usage pattern rather than accomplish
-- it via usage convention and documentation.
clientRW' :: Client
          -> ClientCall
          -> MetadataMap
          -> ClientRWHandler
          -> IO (Either GRPCIOError ClientRWResult)
clientRW' (clientCQ -> cq) (unsafeCC -> c) initMeta f = runExceptT $ do
  sendInitialMetadata c cq initMeta

  -- 'mdmv' is used to synchronize between callers of 'getMD' and 'recv'
  -- below. The behavior of these two operations is different based on their
  -- call order w.r.t. each other, and by whether or not metadata has already
  -- been received.
  --
  -- Regardless of call order, metadata reception is done exactly once. The
  -- result of doing so is cached for subsequent calls to 'getMD'.
  --
  -- 'getMD' will always return the received metadata (or an error if it
  -- occurred), regardless of call order.
  --
  -- When 'getMD' is invoked before 'recv' (and no metadata has been obtained),
  -- metadata is received via a singleton batch, returned, and cached for later
  -- access via 'getMD'. This scenario is analagous to preceding the first read
  -- with WaitForInitialMetadata() in the C++ API.
  --
  -- When 'recv' is invoked before 'getMD' (and no metadata has been obtained),
  -- metadata is received alongside the first payload via an aggregate batch,
  -- and cached for later access via 'getMD'. This scenario is analagous to just
  -- issuing Read() in the C++ API, and having the metadata available via
  -- ClientContext afterwards.
  --
  -- TODO: This is not the whole story about metadata exchange ordering, but
  -- allows us to at least have parity on the client side with the C++ API
  -- w.r.t. when/how metadata is exchanged. We may need to revisit this a bit as
  -- we experiment with other bindings, new GRPC releases come out, and so
  -- forth, but at least this provides us with basic functionality, albeit with
  -- a great deal more caveat programmer than desirable :(

  mdmv <- liftIO (newMVar Nothing)
  let
    getMD = modifyMVar mdmv $ \case
      Just emd -> return (Just emd, emd)
      Nothing  -> do -- getMD invoked before recv
        emd <- runExceptT (recvInitialMetadata c cq)
        return (Just emd, emd)

    recv = modifyMVar mdmv $ \case
      Just emd -> (Just emd,) <$> streamRecvPrim c cq
      Nothing  -> -- recv invoked before getMD
        runExceptT (recvInitialMsgMD c cq) >>= \case
          Left e          -> return (Just (Left e), Left e)
          Right (mbs, md) -> return (Just (Right md), Right mbs)

    send = streamSendPrim c cq

    -- TODO: Regarding usage of writesDone so that there isn't such a burden on
    -- the end user programmer (i.e. must invoke it, and only once): we can just
    -- document this general-purpose function well, and then create slightly
    -- simpler versions of the bidi interface which support (a) monothreaded
    -- send/recv interleaving, with an implicit half-close and (b) separate
    -- send/recv threads, with an implicit half-close after the writer thread
    -- terminates. These simpler versions model the most common use cases
    -- without having to expose the half-close semantics to the end user
    -- programmer.
    writesDone = writesDonePrim c cq

  liftIO (f getMD recv send writesDone)
  recvStatusOnClient c cq -- Finish()

--------------------------------------------------------------------------------
-- clientRequest (client side of normal request/response)

-- | Make a request of the given method with the given body. Returns the
-- server's response.
clientRequest
  :: Client
  -> RegisteredMethod 'Normal
  -> TimeoutSeconds
  -> ByteString
  -- ^ The body of the request
  -> MetadataMap
  -- ^ Metadata to send with the request
  -> IO (Either GRPCIOError NormalRequestResult)
clientRequest c = clientRequestParent c Nothing

-- | Like 'clientRequest', but allows the user to supply an optional parent
-- call, so that call cancellation can be propagated from the parent to the
-- child. This is intended for servers that call other servers.
clientRequestParent
  :: Client
  -> Maybe (ServerCall a)
  -- ^ optional parent call
  -> RegisteredMethod 'Normal
  -> TimeoutSeconds
  -> ByteString
  -- ^ The body of the request
  -> MetadataMap
  -- ^ Metadata to send with the request
  -> IO (Either GRPCIOError NormalRequestResult)
clientRequestParent cl@(clientCQ -> cq) p rm tm body initMeta =
  withClientCallParent cl rm tm p (fmap join . go)
  where
    go (unsafeCC -> c) =
      -- NB: the send and receive operations below *must* be in separate
      -- batches, or the client hangs when the server can't be reached.
      runOps c cq
        [ OpSendInitialMetadata initMeta
        , OpSendMessage body
        , OpSendCloseFromClient
        ]
        >>= \case
          Left x -> do
            grpcDebug "clientRequest(R) : batch error sending."
            return $ Left x
          Right rs ->
            runOps c cq
              [ OpRecvInitialMetadata
              , OpRecvMessage
              , OpRecvStatusOnClient
              ]
              >>= \case
                Left x -> do
                  grpcDebug "clientRequest(R): batch error receiving.."
                  return $ Left x
                Right rs' -> do
                  grpcDebug $ "clientRequest(R): got " ++ show rs'
                  return $ Right $ compileNormalRequestResults (rs ++ rs')

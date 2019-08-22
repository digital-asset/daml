{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module defines data structures and operations pertaining to registered
-- servers using registered calls; for unregistered support, see
-- `Network.GRPC.LowLevel.Server.Unregistered`.
module Network.GRPC.LowLevel.Server where

import           Control.Concurrent                    (ThreadId
                                                        , forkFinally
                                                        , myThreadId
                                                        , killThread)
import           Control.Concurrent.STM                (atomically
                                                        , check)
import           Control.Concurrent.STM.TVar           (TVar
                                                        , modifyTVar'
                                                        , readTVar
                                                        , writeTVar
                                                        , readTVarIO
                                                        , newTVarIO)
import           Control.Exception                     (bracket)
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString                       as B
import qualified Data.Set as S
import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.CompletionQueue (CompletionQueue,
                                                        createCompletionQueue,
                                                        pluck,
                                                        serverRegisterCompletionQueue,
                                                        serverRequestCall,
                                                        serverShutdownAndNotify,
                                                        shutdownCompletionQueue)
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op
import qualified Network.GRPC.Unsafe                   as C
import qualified Network.GRPC.Unsafe.ChannelArgs       as C
import qualified Network.GRPC.Unsafe.Op                as C
import qualified Network.GRPC.Unsafe.Security          as C

-- | Wraps various gRPC state needed to run a server.
data Server = Server
  { serverGRPC           :: GRPC
  , unsafeServer         :: C.Server
  , listeningPort        :: Port
  , serverCQ             :: CompletionQueue
  -- ^ CQ used for receiving new calls.
  , serverCallCQ               :: CompletionQueue
  -- ^ CQ for running ops on calls. Not used to receive new calls.
  , normalMethods        :: [RegisteredMethod 'Normal]
  , sstreamingMethods    :: [RegisteredMethod 'ServerStreaming]
  , cstreamingMethods    :: [RegisteredMethod 'ClientStreaming]
  , bidiStreamingMethods :: [RegisteredMethod 'BiDiStreaming]
  , serverConfig         :: ServerConfig
  , outstandingForks     :: TVar (S.Set ThreadId)
  , serverShuttingDown   :: TVar Bool
  }

-- TODO: should we make a forkGRPC function instead? I am not sure if it would
-- be safe to let the call handlers threads keep running after the server stops,
-- so I'm taking the more conservative route of ensuring the server will
-- stay alive. Experiment more when time permits.
-- | Fork a thread from the server (presumably for a handler) with the guarantee
-- that the server won't shut down while the thread is alive.
-- Returns true if the fork happens successfully, and false if the server is
-- already shutting down (in which case the function to fork is never executed).
-- If the thread stays alive too long while the server is trying to shut down,
-- the thread will be killed with 'killThread'.
-- The purpose of this is to prevent memory access
-- errors at the C level of the library, not to ensure that application layer
-- operations in user code complete successfully.
forkServer :: Server -> IO () -> IO Bool
forkServer Server{..} f = do
  shutdown <- readTVarIO serverShuttingDown
  case shutdown of
    True  -> return False
    False -> do
      -- NB: The spawned thread waits on 'ready' before running 'f' to ensure
      -- that its ThreadId is inserted into outstandingForks before the cleanup
      -- function deletes it from there. Not doing this can lead to stale thread
      -- ids in the set when handlers cleanup ahead of the insertion, and a
      -- subsequent deadlock in stopServer. We can use a dead-list instead if we
      -- need something more performant.
      ready <- newTVarIO False
      tid   <- let act = do atomically (check =<< readTVar ready)
                            f
               in forkFinally act cleanup
      atomically $ do
        modifyTVar' outstandingForks (S.insert tid)
        modifyTVar' ready (const True)
#ifdef DEBUG
      tids <- readTVarIO outstandingForks
      grpcDebug $ "after fork and bookkeeping: outstandingForks=" ++ show tids
#endif
      return True
      where cleanup _ = do
              tid <- myThreadId
              atomically $ modifyTVar' outstandingForks (S.delete tid)

-- | Configuration for SSL.
data ServerSSLConfig = ServerSSLConfig
  {clientRootCert :: Maybe FilePath,
   serverPrivateKey :: FilePath,
   serverCert :: FilePath,
   clientCertRequest :: C.SslClientCertificateRequestType,
   -- ^ Whether to request a certificate from the client, and what to do with it
   -- if received.
   customMetadataProcessor :: Maybe C.ProcessMeta}

-- | Configuration needed to start a server.
data ServerConfig = ServerConfig
  { host              :: Host
    -- ^ Name of the host the server is running on. Not sure how this is
    -- used. Setting to "localhost" works fine in tests.
  , port              :: Port
    -- ^ Port on which to listen for requests.
  , methodsToRegisterNormal :: [MethodName]
    -- ^ List of normal (non-streaming) methods to register.
  , methodsToRegisterClientStreaming :: [MethodName]
  , methodsToRegisterServerStreaming :: [MethodName]
  , methodsToRegisterBiDiStreaming :: [MethodName]
  , serverArgs        :: [C.Arg]
  -- ^ Optional arguments for setting up the channel on the server. Supplying an
  -- empty list will cause the channel to use gRPC's default options.
  , sslConfig :: Maybe ServerSSLConfig
  -- ^ Server-side SSL configuration. If 'Nothing', the server will use an
  -- insecure connection.
  }

serverEndpoint :: ServerConfig -> Endpoint
serverEndpoint ServerConfig{..} = endpoint host port

addPort :: C.Server -> ServerConfig -> IO Int
addPort server conf@ServerConfig{..} =
  case sslConfig of
    Nothing -> C.grpcServerAddInsecureHttp2Port server e
    Just ServerSSLConfig{..} ->
      do crc <- mapM B.readFile clientRootCert
         spk <- B.readFile serverPrivateKey
         sc <- B.readFile serverCert
         C.withServerCredentials crc spk sc clientCertRequest $ \creds -> do
           case customMetadataProcessor of
             Just p -> C.setMetadataProcessor creds p
             Nothing -> return ()
           C.serverAddSecureHttp2Port server e creds
  where e = unEndpoint $ serverEndpoint conf

startServer :: GRPC -> ServerConfig -> IO Server
startServer grpc conf@ServerConfig{..} =
  C.withChannelArgs serverArgs $ \args -> do
    let e = serverEndpoint conf
    server <- C.grpcServerCreate args C.reserved
    actualPort <- addPort server conf
    when (unPort port > 0 && actualPort /= unPort port) $
      error $ "Unable to bind port: " ++ show port
    cq <- createCompletionQueue grpc
    grpcDebug $ "startServer: server CQ: " ++ show cq
    serverRegisterCompletionQueue server cq

    -- Register methods according to their GRPCMethodType kind. It's a bit ugly
    -- to partition them this way, but we get very convenient phantom typing
    -- elsewhere by doing so.
    -- TODO: change order of args so we can eta reduce.
    ns <- mapM (\nm -> serverRegisterMethodNormal server nm e)
               methodsToRegisterNormal
    ss <- mapM (\nm -> serverRegisterMethodServerStreaming server nm e)
               methodsToRegisterServerStreaming
    cs <- mapM (\nm -> serverRegisterMethodClientStreaming server nm e)
               methodsToRegisterClientStreaming
    bs <- mapM (\nm -> serverRegisterMethodBiDiStreaming server nm e)
               methodsToRegisterBiDiStreaming
    C.grpcServerStart server
    forks <- newTVarIO S.empty
    shutdown <- newTVarIO False
    ccq <- createCompletionQueue grpc
    return $ Server grpc server (Port actualPort) cq ccq ns ss cs bs conf forks
      shutdown



stopServer :: Server -> IO ()
-- TODO: Do method handles need to be freed?
stopServer Server{ unsafeServer = s, .. } = do
  grpcDebug "stopServer: calling shutdownNotify."
  shutdownNotify serverCQ
  grpcDebug "stopServer: cancelling all calls."
  C.grpcServerCancelAllCalls s
  cleanupForks
  grpcDebug "stopServer: call grpc_server_destroy."
  C.grpcServerDestroy s
  grpcDebug "stopServer: shutting down CQ."
  shutdownCQ serverCQ
  shutdownCQ serverCallCQ

  where shutdownCQ scq = do
          shutdownResult <- shutdownCompletionQueue scq
          case shutdownResult of
            Left _ -> do putStrLn "Warning: completion queue didn't shut down."
                         putStrLn "Trying to stop server anyway."
            Right _ -> return ()
        shutdownNotify scq = do
          let shutdownTag = C.tag 0
          serverShutdownAndNotify s scq shutdownTag
          grpcDebug "called serverShutdownAndNotify; plucking."
          shutdownEvent <- pluck scq shutdownTag (Just 30)
          grpcDebug $ "shutdownNotify: got shutdown event" ++ show shutdownEvent
          case shutdownEvent of
            -- This case occurs when we pluck but the queue is already in the
            -- 'shuttingDown' state, implying we already tried to shut down.
            Left GRPCIOShutdown -> error "Called stopServer twice!"
            Left _              -> error "Failed to stop server."
            Right _             -> return ()
        cleanupForks = do
          atomically $ writeTVar serverShuttingDown True
          liveForks <- readTVarIO outstandingForks
          grpcDebug $ "Server shutdown: killing threads: " ++ show liveForks
          mapM_ killThread liveForks
          -- wait for threads to shut down
          grpcDebug "Server shutdown: waiting until all threads are dead."
          atomically $ check . (==0) . S.size =<< readTVar outstandingForks
          grpcDebug "Server shutdown: All forks cleaned up."

-- Uses 'bracket' to safely start and stop a server, even if exceptions occur.
withServer :: GRPC -> ServerConfig -> (Server -> IO a) -> IO a
withServer grpc cfg = bracket (startServer grpc cfg) stopServer

-- | Less precisely-typed registration function used in
-- 'serverRegisterMethodNormal', 'serverRegisterMethodServerStreaming',
-- 'serverRegisterMethodClientStreaming', and
-- 'serverRegisterMethodBiDiStreaming'.
serverRegisterMethod :: C.Server
                        -> MethodName
                        -> Endpoint
                        -> GRPCMethodType
                        -> IO C.CallHandle
serverRegisterMethod s nm e mty =
  C.grpcServerRegisterMethod s
                             (unMethodName nm)
                             (unEndpoint e)
                             (payloadHandling mty)

{-
TODO: Consolidate the register functions below.

It seems like we'd need true dependent types to use only one
  registration function. Ideally we'd want a type like
  serverRegisterMethod :: C.Server
                          -> MethodName
                          -> Endpoint
                          -> (t :: GRPCMethodType)
                          -> IO (RegisteredMethod (Lifted t))

where `Lifted t` is the type in the t data kind that corresponds to the data
constructor t the function was given.

-}

-- | Register a method on a server. The 'RegisteredMethod' type can then be used
-- to wait for a request to arrive. Note: gRPC claims this must be called before
-- the server is started, so we do it during startup according to the
-- 'ServerConfig'.
serverRegisterMethodNormal :: C.Server
                     -> MethodName
                     -- ^ method name, e.g. "/foo"
                     -> Endpoint
                     -- ^ Endpoint name name, e.g. "localhost:9999". I have no
                     -- idea why this is needed since we have to provide these
                     -- parameters to start a server in the first place. It
                     -- doesn't seem to have any effect, even if it's filled
                     -- with nonsense.
                     -> IO (RegisteredMethod 'Normal)
serverRegisterMethodNormal internalServer meth e = do
  h <- serverRegisterMethod internalServer meth e Normal
  return $ RegisteredMethodNormal meth e h

serverRegisterMethodClientStreaming
  :: C.Server
     -> MethodName
     -- ^ method name, e.g. "/foo"
     -> Endpoint
     -- ^ Endpoint name name, e.g. "localhost:9999". I have no
     -- idea why this is needed since we have to provide these
     -- parameters to start a server in the first place. It
     -- doesn't seem to have any effect, even if it's filled
     -- with nonsense.
     -> IO (RegisteredMethod 'ClientStreaming)
serverRegisterMethodClientStreaming internalServer meth e = do
  h <- serverRegisterMethod internalServer meth e ClientStreaming
  return $ RegisteredMethodClientStreaming meth e h


serverRegisterMethodServerStreaming
  :: C.Server
     -> MethodName
     -- ^ method name, e.g. "/foo"
     -> Endpoint
     -- ^ Endpoint name name, e.g. "localhost:9999". I have no
     -- idea why this is needed since we have to provide these
     -- parameters to start a server in the first place. It
     -- doesn't seem to have any effect, even if it's filled
     -- with nonsense.
     -> IO (RegisteredMethod 'ServerStreaming)
serverRegisterMethodServerStreaming internalServer meth e = do
  h <- serverRegisterMethod internalServer meth e ServerStreaming
  return $ RegisteredMethodServerStreaming meth e h


serverRegisterMethodBiDiStreaming
  :: C.Server
     -> MethodName
     -- ^ method name, e.g. "/foo"
     -> Endpoint
     -- ^ Endpoint name name, e.g. "localhost:9999". I have no
     -- idea why this is needed since we have to provide these
     -- parameters to start a server in the first place. It
     -- doesn't seem to have any effect, even if it's filled
     -- with nonsense.
     -> IO (RegisteredMethod 'BiDiStreaming)
serverRegisterMethodBiDiStreaming internalServer meth e = do
  h <- serverRegisterMethod internalServer meth e BiDiStreaming
  return $ RegisteredMethodBiDiStreaming meth e h

-- | Create a 'Call' with which to wait for the invocation of a registered
-- method.
serverCreateCall :: Server
                 -> RegisteredMethod mt
                 -> IO (Either GRPCIOError (ServerCall (MethodPayload mt)))
serverCreateCall Server{..} rm =
  serverRequestCall rm unsafeServer serverCQ serverCallCQ

withServerCall :: Server
               -> RegisteredMethod mt
               -> (ServerCall (MethodPayload mt) -> IO (Either GRPCIOError a))
               -> IO (Either GRPCIOError a)
withServerCall s rm f =
  bracket (serverCreateCall s rm) cleanup $ \case
    Left e  -> return (Left e)
    Right c -> do
      debugServerCall c
      f c
  where
    cleanup (Left _) = pure ()
    cleanup (Right c) = do
      grpcDebug "withServerCall(R): destroying."
      destroyServerCall c

--------------------------------------------------------------------------------
-- serverReader (server side of client streaming mode)

type ServerReaderHandlerLL
  =  ServerCall (MethodPayload 'ClientStreaming)
  -> StreamRecv ByteString
  -> IO (Maybe ByteString, MetadataMap, C.StatusCode, StatusDetails)

serverReader :: Server
             -> RegisteredMethod 'ClientStreaming
             -> MetadataMap -- ^ Initial server metadata
             -> ServerReaderHandlerLL
             -> IO (Either GRPCIOError ())
serverReader s rm initMeta f =
  withServerCall s rm (\sc -> serverReader' s sc initMeta f)

serverReader' :: Server
              -> ServerCall (MethodPayload 'ClientStreaming)
              -> MetadataMap -- ^ Initial server metadata
              -> ServerReaderHandlerLL
              -> IO (Either GRPCIOError ())
serverReader' _ sc@ServerCall{ unsafeSC = c, callCQ = ccq } initMeta f =
  runExceptT $ do
    (mmsg, trailMeta, st, ds) <- liftIO $ f sc (streamRecvPrim c ccq)
    void $ runOps' c ccq ( OpSendInitialMetadata initMeta
                         : OpSendStatusFromServer trailMeta st ds
                         : maybe [] ((:[]) . OpSendMessage) mmsg
                         )

--------------------------------------------------------------------------------
-- serverWriter (server side of server streaming mode)

type ServerWriterHandlerLL
  =  ServerCall (MethodPayload 'ServerStreaming)
  -> StreamSend ByteString
  -> IO (MetadataMap, C.StatusCode, StatusDetails)

-- | Wait for and then handle a registered, server-streaming call.
serverWriter :: Server
             -> RegisteredMethod 'ServerStreaming
             -> MetadataMap -- ^ Initial server metadata
             -> ServerWriterHandlerLL
             -> IO (Either GRPCIOError ())
serverWriter s rm initMeta f =
  withServerCall s rm (\sc -> serverWriter' s sc initMeta f)

serverWriter' :: Server
              -> ServerCall (MethodPayload 'ServerStreaming)
              -> MetadataMap
              -> ServerWriterHandlerLL
              -> IO (Either GRPCIOError ())
serverWriter' _ sc@ServerCall{ unsafeSC = c, callCQ = ccq } initMeta f =
  runExceptT $ do
    sendInitialMetadata c ccq initMeta
    st <- liftIO $ f sc (streamSendPrim c ccq)
    sendStatusFromServer c ccq st

--------------------------------------------------------------------------------
-- serverRW (bidirectional streaming mode)

type ServerRWHandlerLL
  =  ServerCall (MethodPayload 'BiDiStreaming)
  -> StreamRecv ByteString
  -> StreamSend ByteString
  -> IO (MetadataMap, C.StatusCode, StatusDetails)

serverRW :: Server
         -> RegisteredMethod 'BiDiStreaming
         -> MetadataMap -- ^ initial server metadata
         -> ServerRWHandlerLL
         -> IO (Either GRPCIOError ())
serverRW s rm initMeta f =
  withServerCall s rm (\sc -> serverRW' s sc initMeta f)

serverRW' :: Server
          -> ServerCall (MethodPayload 'BiDiStreaming)
          -> MetadataMap
          -> ServerRWHandlerLL
          -> IO (Either GRPCIOError ())
serverRW' _ sc@ServerCall{ unsafeSC = c, callCQ = ccq } initMeta f =
  runExceptT $ do
    sendInitialMetadata c ccq initMeta
    st <- liftIO $ f sc (streamRecvPrim c ccq) (streamSendPrim c ccq)
    sendStatusFromServer c ccq st

--------------------------------------------------------------------------------
-- serverHandleNormalCall (server side of normal request/response)

-- | A handler for a registered server call; bytestring parameter is request
-- body, with the bytestring response body in the result tuple. The first
-- metadata parameter refers to the request metadata, with the two metadata
-- values in the result tuple being the initial and trailing metadata
-- respectively. We pass in the 'ServerCall' so that the server can call
-- 'serverCallCancel' on it if needed.
type ServerHandlerLL
  =  ServerCall (MethodPayload 'Normal)
  -> IO (ByteString, MetadataMap, C.StatusCode, StatusDetails)

-- | Wait for and then handle a normal (non-streaming) call.
serverHandleNormalCall :: Server
                       -> RegisteredMethod 'Normal
                       -> MetadataMap
                       -- ^ Initial server metadata
                       -> ServerHandlerLL
                       -> IO (Either GRPCIOError ())
serverHandleNormalCall s rm initMeta f =
  withServerCall s rm go
  where
    go sc@ServerCall{ unsafeSC = c, callCQ = ccq } = do
      (rsp, trailMeta, st, ds) <- f sc
      void <$> runOps c ccq [ OpSendInitialMetadata initMeta
                            , OpRecvCloseOnServer
                            , OpSendMessage rsp
                            , OpSendStatusFromServer trailMeta st ds
                            ]

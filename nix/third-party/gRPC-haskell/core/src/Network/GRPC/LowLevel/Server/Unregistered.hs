{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}

module Network.GRPC.LowLevel.Server.Unregistered where

import           Control.Exception                                  (bracket, finally, mask)
import           Control.Monad
import           Control.Monad.Trans.Except
import           Data.ByteString                                    (ByteString)
import           Network.GRPC.LowLevel.Call.Unregistered
import           Network.GRPC.LowLevel.CompletionQueue.Unregistered (serverRequestCall)
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op
import           Network.GRPC.LowLevel.Server                       (Server (..),
                                                                     ServerRWHandlerLL,
                                                                     ServerReaderHandlerLL,
                                                                     ServerWriterHandlerLL,
                                                                     forkServer,
                                                                     serverReader',
                                                                     serverWriter',
                                                                     serverRW')
import qualified Network.GRPC.Unsafe.Op                             as C

serverCreateCall :: Server
                 -> IO (Either GRPCIOError ServerCall)
serverCreateCall Server{..} =
  serverRequestCall unsafeServer serverCQ serverCallCQ

withServerCall :: Server
               -> (ServerCall -> IO (Either GRPCIOError a))
               -> IO (Either GRPCIOError a)
withServerCall s f =
  bracket (serverCreateCall s) cleanup $ \case
    Left e -> return (Left e)
    Right c -> f c
  where
    cleanup (Left _) = pure ()
    cleanup (Right c) = do
      grpcDebug "withServerCall: destroying."
      destroyServerCall c

-- | Gets a call and then forks the given function on a new thread, with the
-- new call as input. Blocks until a call is received, then returns immediately.
-- Handles cleaning up the call safely.
-- Because this function doesn't wait for the handler to return, it cannot
-- return errors.
withServerCallAsync :: Server
                    -> (ServerCall -> IO ())
                    -> IO ()
withServerCallAsync s f = mask $ \unmask ->
  serverCreateCall s >>= \case
    Left e -> do grpcDebug $ "withServerCallAsync: call error: " ++ show e
                 return ()
    Right c -> do wasForkSuccess <- forkServer s handler
                  unless wasForkSuccess destroy
                where handler = unmask (f c) `finally` destroy
                      -- TODO: We sometimes never finish cleanup if the server
                      -- is shutting down and calls killThread. This causes gRPC
                      -- core to complain about leaks.  I think the cause of
                      -- this is that killThread gets called after we are
                      -- already in destroyServerCall, and wrapping
                      -- uninterruptibleMask doesn't seem to help.  Doesn't
                      -- crash, but does emit annoying log messages.
                      destroy = do
                        grpcDebug "withServerCallAsync: destroying."
                        destroyServerCall c
                        grpcDebug "withServerCallAsync: cleanup finished."

-- | A handler for an unregistered server call; bytestring arguments are the
-- request body and response body respectively.
type ServerHandler
  =  ServerCall
  -> ByteString
  -> IO (ByteString, MetadataMap, C.StatusCode, StatusDetails)

-- | Handle one unregistered call.
serverHandleNormalCall :: Server
                       -> MetadataMap -- ^ Initial server metadata.
                       -> ServerHandler
                       -> IO (Either GRPCIOError ())
serverHandleNormalCall s initMeta f =
  withServerCall s $ \c -> serverHandleNormalCall' s c initMeta f

serverHandleNormalCall' :: Server
                        -> ServerCall
                        -> MetadataMap -- ^ Initial server metadata.
                        -> ServerHandler
                        -> IO (Either GRPCIOError ())
serverHandleNormalCall'
  _ sc@ServerCall{ unsafeSC = c, callCQ = cq, .. } initMeta f = do
      grpcDebug "serverHandleNormalCall(U): starting batch."
      runOps c cq
        [ OpSendInitialMetadata initMeta
        , OpRecvMessage
        ]
        >>= \case
          Left x -> do
            grpcDebug "serverHandleNormalCall(U): ops failed; aborting"
            return $ Left x
          Right [OpRecvMessageResult (Just body)] -> do
            grpcDebug $ "got client metadata: " ++ show metadata
            grpcDebug $ "call_details host is: " ++ show callHost
            (rsp, trailMeta, st, ds) <- f sc body
            -- TODO: We have to put 'OpRecvCloseOnServer' in the response ops,
            -- or else the client times out. Given this, I have no idea how to
            -- check for cancellation on the server.
            runOps c cq
              [ OpRecvCloseOnServer
              , OpSendMessage rsp,
                OpSendStatusFromServer trailMeta st ds
              ]
              >>= \case
                Left x -> do
                  grpcDebug "serverHandleNormalCall(U): resp failed."
                  return $ Left x
                Right _ -> do
                  grpcDebug "serverHandleNormalCall(U): ops done."
                  return $ Right ()
          x -> error $ "impossible pattern match: " ++ show x

serverReader :: Server
             -> ServerCall
             -> MetadataMap -- ^ Initial server metadata
             -> ServerReaderHandlerLL
             -> IO (Either GRPCIOError ())
serverReader s = serverReader' s . convertCall

serverWriter :: Server
             -> ServerCall
             -> MetadataMap -- ^ Initial server metadata
             -> ServerWriterHandlerLL
             -> IO (Either GRPCIOError ())
serverWriter s sc@ServerCall{ unsafeSC = c, callCQ = ccq } initMeta f =
  runExceptT $ do
    bs <- recvInitialMessage c ccq
    ExceptT (serverWriter' s (const bs <$> convertCall sc) initMeta f)

serverRW :: Server
         -> ServerCall
         -> MetadataMap -- ^ Initial server metadata
         -> ServerRWHandlerLL
         -> IO (Either GRPCIOError ())
serverRW s = serverRW' s . convertCall

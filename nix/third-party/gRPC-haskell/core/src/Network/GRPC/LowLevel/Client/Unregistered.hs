{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns    #-}

module Network.GRPC.LowLevel.Client.Unregistered where

import           Control.Arrow
import           Control.Exception                                  (bracket)
import           Control.Monad                                      (join)
import           Data.ByteString                                    (ByteString)
import           Foreign.Ptr                                        (nullPtr)
import qualified Network.GRPC.Unsafe                                as C
import qualified Network.GRPC.Unsafe.Constants                      as C
import qualified Network.GRPC.Unsafe.Time                           as C

import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.Client                       (Client (..),
                                                                     NormalRequestResult (..),
                                                                     clientEndpoint,
                                                                     compileNormalRequestResults)
import           Network.GRPC.LowLevel.CompletionQueue              (TimeoutSeconds)
import qualified Network.GRPC.LowLevel.CompletionQueue.Unregistered as U
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op

-- | Create a call on the client for an endpoint without using the
-- method registration machinery. In practice, we'll probably only use the
-- registered method version, but we include this for completeness and testing.
clientCreateCall :: Client
                 -> MethodName
                 -> TimeoutSeconds
                 -> IO (Either GRPCIOError ClientCall)
clientCreateCall Client{..} meth timeout = do
  let parentCall = C.Call nullPtr
  C.withDeadlineSeconds timeout $ \deadline -> do
    U.channelCreateCall clientChannel parentCall C.propagateDefaults
      clientCQ meth (clientEndpoint clientConfig) deadline

withClientCall :: Client
               -> MethodName
               -> TimeoutSeconds
               -> (ClientCall -> IO (Either GRPCIOError a))
               -> IO (Either GRPCIOError a)
withClientCall client method timeout f =
  bracket (clientCreateCall client method timeout) cleanup $ \case
    Left x -> return $ Left x
    Right call -> f call
  where
    cleanup (Left _) = pure ()
    cleanup (Right call) = do
      grpcDebug "withClientCall(U): destroying."
      destroyClientCall call

-- | Makes a normal (non-streaming) request without needing to register a method
-- first. Probably only useful for testing.
clientRequest :: Client
              -> MethodName
              -- ^ Method name, e.g. "/foo"
              -> TimeoutSeconds
              -- ^ "Number of seconds until request times out"
              -> ByteString
              -- ^ Request body.
              -> MetadataMap
              -- ^ Request metadata.
              -> IO (Either GRPCIOError NormalRequestResult)
clientRequest cl@(clientCQ -> cq) meth tm body initMeta =
  join <$> withClientCall cl meth tm go
  where
    go (unsafeCC -> c) = do
      results <- runOps c cq
                   [ OpSendInitialMetadata initMeta
                   , OpSendMessage body
                   , OpSendCloseFromClient
                   , OpRecvInitialMetadata
                   , OpRecvMessage
                   , OpRecvStatusOnClient
                   ]
      grpcDebug "clientRequest(U): ops ran."
      return $ right compileNormalRequestResults results

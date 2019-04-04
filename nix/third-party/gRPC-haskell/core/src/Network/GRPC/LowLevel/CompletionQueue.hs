-- | Unlike most of the other internal low-level modules, we don't export
-- everything here. There are several things in here that, if accessed, could
-- cause race conditions, so we only expose functions that are thread safe.
-- However, some of the functions we export here can cause memory leaks if used
-- improperly.
--
-- When definition operations which pertain to calls, this module only provides
-- definitions for registered calls; for unregistered variants, see
-- `Network.GRPC.LowLevel.CompletionQueue.Unregistered`. Type definitions and
-- implementation details to both are kept in
-- `Network.GRPC.LowLevel.CompletionQueue.Internal`.

{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}

module Network.GRPC.LowLevel.CompletionQueue
  ( CompletionQueue
  , withCompletionQueue
  , createCompletionQueue
  , shutdownCompletionQueue
  , pluck
  , startBatch
  , channelCreateCall
  , TimeoutSeconds
  , isEventSuccessful
  , serverRegisterCompletionQueue
  , serverShutdownAndNotify
  , serverRequestCall
  , newTag
  )
where

import           Control.Concurrent.STM.TVar                    (newTVarIO)
import           Control.Exception                              (bracket)
import           Control.Monad.Managed
import           Control.Monad.Trans.Class                      (MonadTrans (lift))
import           Control.Monad.Trans.Except
import           Data.IORef                                     (newIORef)
import           Data.List                                      (intersperse)
import           Foreign.Ptr                                    (nullPtr)
import           Foreign.Storable                               (peek)
import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.CompletionQueue.Internal
import           Network.GRPC.LowLevel.GRPC
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Constants                  as C
import qualified Network.GRPC.Unsafe.Metadata                   as C
import qualified Network.GRPC.Unsafe.Op                         as C
import qualified Network.GRPC.Unsafe.Time                       as C
import           System.Clock                                   (Clock (..),
                                                                 getTime)
import           System.Info                                    (os)

withCompletionQueue :: GRPC -> (CompletionQueue -> IO a) -> IO a
withCompletionQueue grpc = bracket (createCompletionQueue grpc)
                                   shutdownCompletionQueue

createCompletionQueue :: GRPC -> IO CompletionQueue
createCompletionQueue _ = do
  unsafeCQ <- C.grpcCompletionQueueCreateForPluck C.reserved
  currentPluckers <- newTVarIO 0
  currentPushers <- newTVarIO 0
  shuttingDown <- newTVarIO False
  nextTag <- newIORef minBound
  return CompletionQueue{..}

-- | Very simple wrapper around 'grpcCallStartBatch'. Throws 'GRPCIOShutdown'
-- without calling 'grpcCallStartBatch' if the queue is shutting down.
-- Throws 'CallError' if 'grpcCallStartBatch' returns a non-OK code.
startBatch :: CompletionQueue -> C.Call -> C.OpArray -> Int -> C.Tag
              -> IO (Either GRPCIOError ())
startBatch cq@CompletionQueue{..} call opArray opArraySize tag =
    withPermission Push cq $ fmap throwIfCallError $ do
      grpcDebug $ "startBatch: calling grpc_call_start_batch with pointers: "
                  ++ show call ++ " " ++ show opArray
      res <- C.grpcCallStartBatch call opArray opArraySize tag C.reserved
      grpcDebug "startBatch: grpc_call_start_batch call returned."
      return res

channelCreateCall :: C.Channel
                  -> Maybe (ServerCall a)
                  -> C.PropagationMask
                  -> CompletionQueue
                  -> C.CallHandle
                  -> C.CTimeSpecPtr
                  -> IO (Either GRPCIOError ClientCall)
channelCreateCall
  chan parent mask cq@CompletionQueue{..} handle deadline =
  withPermission Push cq $ do
    let parentPtr = maybe (C.Call nullPtr) unsafeSC parent
    grpcDebug $ "channelCreateCall: call with "
                ++ concat (intersperse " " [show chan, show parentPtr,
                                            show mask,
                                            show unsafeCQ, show handle,
                                            show deadline])
    call <- C.grpcChannelCreateRegisteredCall chan parentPtr mask unsafeCQ
                                              handle deadline C.reserved
    return $ Right $ ClientCall call

-- | Create the call object to handle a registered call.
serverRequestCall :: RegisteredMethod mt
                  -> C.Server
                  -> CompletionQueue -- ^ server CQ
                  -> CompletionQueue -- ^ call CQ
                  -> IO (Either GRPCIOError (ServerCall (MethodPayload mt)))
serverRequestCall rm s scq ccq =
  -- NB: The method type dictates whether or not a payload is present, according
  -- to the payloadHandling function. We do not allocate a buffer for the
  -- payload when it is not present.
  withPermission Push scq . with allocs $ \(dead, call, pay, meta) ->
    withPermission Pluck scq $ do
      md  <- peek meta
      tag <- newTag scq
      dbug $ "got pluck permission, registering call for tag=" ++ show tag
      ce <- C.grpcServerRequestRegisteredCall s (methodHandle rm) call dead md
              pay (unsafeCQ ccq) (unsafeCQ scq) tag
      runExceptT $ case ce of
        C.CallOk -> do
          ExceptT $ do
            r <- pluck' scq tag Nothing
            dbug $ "pluck' finished:" ++ show r
            return r
          lift $
            ServerCall
            <$> peek call
            <*> return ccq
            <*> C.getAllMetadataArray md
            <*> extractPayload rm pay
            <*> convertDeadline dead
        _ -> do
          lift $ dbug $ "Throwing callError: " ++ show ce
          throwE (GRPCIOCallError ce)
  where
    allocs = (,,,)
      <$> mgdPtr
      <*> mgdPtr
      <*> mgdPayload (methodType rm)
      <*> managed C.withMetadataArrayPtr
    dbug = grpcDebug . ("serverRequestCall(R): " ++)
    -- On OS X, gRPC gives us a deadline that is just a delta, so we convert
    -- it to an actual deadline.
    convertDeadline (fmap C.timeSpec . peek -> d)
      | os == "darwin" = (+) <$> d <*> getTime Monotonic
      | otherwise      = d

-- | Register the server's completion queue. Must be done before the server is
-- started.
serverRegisterCompletionQueue :: C.Server -> CompletionQueue -> IO ()
serverRegisterCompletionQueue server CompletionQueue{..} =
  C.grpcServerRegisterCompletionQueue server unsafeCQ C.reserved

serverShutdownAndNotify :: C.Server -> CompletionQueue -> C.Tag -> IO ()
serverShutdownAndNotify server CompletionQueue{..} tag =
  C.grpcServerShutdownAndNotify server unsafeCQ tag

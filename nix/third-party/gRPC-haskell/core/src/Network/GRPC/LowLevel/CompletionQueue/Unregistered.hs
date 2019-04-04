{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}

module Network.GRPC.LowLevel.CompletionQueue.Unregistered where

import           Control.Monad.Managed
import           Control.Monad.Trans.Class                      (MonadTrans (lift))
import           Control.Monad.Trans.Except
import           Foreign.Storable                               (peek)
import           Network.GRPC.LowLevel.Call
import qualified Network.GRPC.LowLevel.Call.Unregistered        as U
import           Network.GRPC.LowLevel.CompletionQueue.Internal
import           Network.GRPC.LowLevel.GRPC
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Constants                  as C
import qualified Network.GRPC.Unsafe.Metadata                   as C
import qualified Network.GRPC.Unsafe.Time                       as C

channelCreateCall :: C.Channel
                  -> C.Call
                  -> C.PropagationMask
                  -> CompletionQueue
                  -> MethodName
                  -> Endpoint
                  -> C.CTimeSpecPtr
                  -> IO (Either GRPCIOError ClientCall)
channelCreateCall chan parent mask cq@CompletionQueue{..} meth endpt deadline =
  withPermission Push cq $ do
    call <- C.grpcChannelCreateCall chan parent mask unsafeCQ
              (unMethodName meth) (unEndpoint endpt) deadline C.reserved
    return $ Right $ ClientCall call


serverRequestCall :: C.Server
                  -> CompletionQueue -- ^ server CQ / notification CQ
                  -> CompletionQueue -- ^ call CQ
                  -> IO (Either GRPCIOError U.ServerCall)
serverRequestCall s scq ccq =
  withPermission Push scq . with allocs $ \(call, meta, cd) ->
    withPermission Pluck scq $ do
      md  <- peek meta
      tag <- newTag scq
      dbug $ "got pluck permission, registering call for tag=" ++ show tag
      ce  <- C.grpcServerRequestCall s call cd md (unsafeCQ ccq) (unsafeCQ scq) tag
      runExceptT $ case ce of
        C.CallOk -> do
          ExceptT $ do
            let
              rec = do
                -- yield every second, for interruptibility
                r <- pluck' scq tag (Just 1)
                case r of
                  Left GRPCIOTimeout -> rec
                  _ -> return r
            r <- rec
            dbug $ "pluck' finished: " ++ show r
            return r
          lift $
            U.ServerCall
              <$> peek call
              <*> return ccq
              <*> C.getAllMetadataArray md
              <*> (C.timeSpec <$> C.callDetailsGetDeadline cd)
              <*> (MethodName <$> C.callDetailsGetMethod   cd)
              <*> (Host       <$> C.callDetailsGetHost     cd)
        _ -> do
          lift $ dbug $ "Throwing callError: " ++ show ce
          throwE $ GRPCIOCallError ce
  where
    allocs = (,,)
      <$> mgdPtr
      <*> managed C.withMetadataArrayPtr
      <*> managed C.withCallDetails
    dbug = grpcDebug . ("serverRequestCall(U): " ++)

-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.JsonRpc
    ( runServer
    , FromRequest(..)
    , ToRequest(..)
    , ErrorObj(..)
    ) where


import DA.Prelude


import           Control.Concurrent.Async.Lifted
import           Control.Concurrent.STM

import           DA.Service.JsonRpc.Data
import           DA.Service.JsonRpc.Interface
import qualified DA.Service.Logger as Logger

import qualified Data.Aeson as Aeson
import           Data.ByteString (ByteString)
import           Data.Conduit
import           Data.Conduit.TMChan
import qualified Data.Text.Extended as T


-- | Internal server events
data Event = EventRequest Request | EventNop | EventChannelClosed

-- | Run the server on given sink and source with given handlers.
runServer
    :: (FromRequest req, FromRequest notif,
        ToRequest outNotif, Aeson.ToJSON outNotif,
        Aeson.ToJSON resp)
    => Logger.Handle IO
    -> ConduitT ByteString Void IO () -- ^ Sink to send messages
    -> ConduitT () ByteString IO () -- ^ Source to receive messages from
    -> TChan outNotif
    -> (req   -> IO (Either ErrorObj resp)) -- ^ Request handler
    -> (notif -> IO ())      -- ^ Notification handler
    -> IO ()
runServer loggerH snk src notifChan requestHandler notificationHandler = do
    qs <- atomically $ initSession V2
    let inSnk  = sinkTBMChan (inCh qs)
        outChan = outCh qs
        outSrc = sourceTBMChan outChan
        reqChan = reqCh qs
    void . waitAnyCancel =<< traverse async
      [ runConduit $ src .| decodeConduit V2 .| inSnk
      , runConduit $ outSrc .| encodeConduit .| snk
      , processIncoming loggerH qs
      , processEvents reqChan outChan
      ]

  where
    receiveSingleRequest :: TBMChan BatchRequest -> STM Event
    receiveSingleRequest reqChan = do
        batch <- readTBMChan reqChan
        return $ case batch of
          Nothing -> EventChannelClosed
          Just (SingleRequest q) -> EventRequest q

          -- ISSUE DEL-3282: In the JsonRPC implementation,
          -- batch requests are being dropped on the floor.
          -- We should at least log a proper error.
          Just BatchRequest{}    -> EventNop

    sendNotification :: TBMChan Message -> STM Event
    sendNotification outChan = do
        note <- readTChan notifChan
        let msg = MsgRequest $ Notif V2 (requestMethod note) (Aeson.toJSON note)
        writeTBMChan outChan msg
        return EventNop

    processEvents :: TBMChan BatchRequest -> TBMChan Message -> IO ()
    processEvents reqChan outChan = do
        event <- liftIO $ atomically $
          receiveSingleRequest reqChan
          `orElse`
          sendNotification outChan

        case event of
          EventRequest rpcReq@(Request ver _ _ rpcId) -> do
              case fromRequest rpcReq of
                Left err ->
                    Logger.logError loggerH $ "Failed to parse request: " <> T.show err

                Right request -> do
                    response <- requestHandler request
                    liftIO . atomically . writeTBMChan outChan $
                      case response of
                        Left err ->
                            MsgResponse $ ResponseError ver err rpcId

                        Right result ->
                            MsgResponse $ Response ver (Aeson.toJSON result) rpcId

              processEvents reqChan outChan

          EventRequest rpcNotif@(Notif _ _ _) -> do
              case fromRequest rpcNotif of
                Left err -> do
                    Logger.logWarning loggerH $ "Failed to parse notification: " <> T.show err
                    return ()
                Right notif -> do
                    notificationHandler notif
              processEvents reqChan outChan

          EventChannelClosed  -> do
              Logger.logInfo loggerH "Channel closed, terminating."
              return ()

          EventNop ->
              processEvents reqChan outChan



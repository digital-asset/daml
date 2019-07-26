-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}

module DA.Ledger.GrpcWrapUtils (
    noTrace, emptyMdm,
    unwrap, unwrapWithNotFound, unwrapWithInvalidArgument,
    sendToStream,
    ) where

import Prelude hiding (fail)

import Com.Digitalasset.Ledger.Api.V1.TraceContext (TraceContext)
import Control.Exception (throwIO)
import Control.Monad.Fail (fail)
import Control.Monad.Fix (fix)
import DA.Ledger.Stream
import DA.Ledger.Convert(Perhaps)
import Network.GRPC.HighLevel (clientCallCancel)
import Network.GRPC.HighLevel.Generated
import qualified Data.Map as Map

noTrace :: Maybe TraceContext
noTrace = Nothing

emptyMdm :: MetadataMap
emptyMdm = MetadataMap Map.empty

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

unwrapWithNotFound :: ClientResult 'Normal a -> IO (Maybe a)
unwrapWithNotFound = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return $ Just x
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _)) -> return Nothing
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

unwrapWithInvalidArgument :: ClientResult 'Normal a -> IO (Either StatusDetails a)
unwrapWithInvalidArgument = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return $ Right x
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) -> return $ Left details
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

sendToStream :: Show b => Int -> a -> (b -> Perhaps c) -> Stream c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToStream timeout request f stream rpc = do
    res <- rpc $
        ClientReaderRequestCC request timeout emptyMdm
        (\cc -> onClose stream $ \_cancel -> clientCallCancel cc)
        $ \_mdm recv -> do
          fix $ \again -> do
            recv >>= \case
                Left e -> do
                    writeStream stream (Left (Abnormal (show e)))
                    return ()
                Right Nothing -> do
                    return ()
                Right (Just b) ->
                    case f b of
                        Left reason -> do
                            let mes = "convert failed: " <> show reason <> ":\n" <> show b
                            writeStream stream (Left (Abnormal mes))
                        Right cs -> do
                            writeStream stream $ Right cs
                            again
    case res of
        ClientReaderResponse _meta StatusOk _details -> do
            writeStream stream (Left EOS)
        ClientReaderResponse _meta code details -> do
            writeStream stream (Left (Abnormal (show (code,details))))
        ClientErrorResponse e -> do
            writeStream stream (Left (Abnormal (show e)))

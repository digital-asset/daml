-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Ledger.GrpcWrapUtils (
    unwrap, unwrapWithNotFound, unwrapWithInvalidArgument,
    sendToStream,
    ) where

import Prelude hiding (fail)

import Control.Exception (throwIO)
import Control.Monad.Fail (fail)
import Control.Monad.Fix (fix)
import DA.Ledger.Stream
import DA.Ledger.Convert (Perhaps,runRaise)
import Network.GRPC.HighLevel (clientCallCancel)
import Network.GRPC.HighLevel.Generated

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

unwrapWithInvalidArgument :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithInvalidArgument = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return $ Right x
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) -> return $ Left $ show $ unStatusDetails details
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

sendToStream :: Show b => Int -> MetadataMap -> a -> (b -> Perhaps c) -> Stream c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToStream timeout mdm request convertResponse stream rpc = do
    res <- rpc $
        ClientReaderRequest request timeout mdm
        $ \clientCall _mdm recv -> do
          onClose stream $ \_ -> clientCallCancel clientCall
          fix $ \again -> do
            recv >>= \case
                Left e ->  failToStream (show e)
                Right Nothing -> return ()
                Right (Just b) -> runRaise convertResponse b >>= \case
                    Left reason ->
                        failToStream $ show reason
                    Right c -> do
                        writeStream stream $ Right c
                        again
    case res of
        ClientReaderResponse _meta StatusOk _details ->
            writeStream stream (Left EOS)
        ClientReaderResponse _meta code details ->
            failToStream $ show (code,details)
        ClientErrorResponse e ->
            failToStream $ show e
  where
      failToStream :: String -> IO ()
      failToStream msg = writeStream stream (Left (Abnormal msg))

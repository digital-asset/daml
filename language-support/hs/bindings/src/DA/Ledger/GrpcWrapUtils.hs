-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Ledger.GrpcWrapUtils (
    unwrap, unwrapWithNotFound, unwrapWithInvalidArgument,
    unwrapWithCommandSubmissionFailure,
    unwrapWithTransactionFailures,
    sendToStream,
    ) where

import Prelude hiding (fail)

import Control.Exception (throwIO)
import Control.Monad.Fail (fail)
import Control.Monad.Fix (fix)
import DA.Ledger.Stream
import DA.Ledger.Convert (Perhaps,runRaise)
import Data.Either.Extra (eitherToMaybe)
import Network.GRPC.HighLevel (clientCallCancel)
import Network.GRPC.HighLevel.Generated

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

unwrapWithExpectedFailures :: [StatusCode] -> ClientResult 'Normal a -> IO (Either String a)
unwrapWithExpectedFailures errs = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return $ Right x
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode code details))
        | code `elem` errs ->
          return $ Left $ show $ unStatusDetails details
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

unwrapWithNotFound :: ClientResult 'Normal a -> IO (Maybe a)
unwrapWithNotFound = fmap eitherToMaybe . unwrapWithExpectedFailures [StatusNotFound]

unwrapWithInvalidArgument :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithInvalidArgument = unwrapWithExpectedFailures [StatusInvalidArgument]

unwrapWithCommandSubmissionFailure :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithCommandSubmissionFailure =
    unwrapWithExpectedFailures [StatusInvalidArgument, StatusNotFound, StatusFailedPrecondition, StatusAlreadyExists]

unwrapWithTransactionFailures :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithTransactionFailures =
    unwrapWithExpectedFailures [StatusInvalidArgument, StatusNotFound]

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

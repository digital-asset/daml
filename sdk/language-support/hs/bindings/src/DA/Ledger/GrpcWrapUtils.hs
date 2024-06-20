-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Ledger.GrpcWrapUtils (
    unwrap, unwrapWithNotFound,
    unwrapWithInvalidArgument,
    unwrapWithInvalidArgumentAndMetadata,
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
import DA.Ledger.Types (Status (..), ErrorInfo (..))
import Data.Bifunctor (bimap, first)
import Data.ByteString.UTF8 (toString)
import Data.Either.Extra (eitherToMaybe)
import Data.List (find)
import qualified Data.Map as Map
import Data.Text.Lazy (unpack)
import Network.GRPC.HighLevel (clientCallCancel)
import Network.GRPC.HighLevel.Generated
import Network.GRPC.LowLevel.GRPC.MetadataMap (lookupLast)
import Proto3.Suite (fromByteString)
import Google.Protobuf.Any (Any (..))

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

extractErrorInfoMetadata :: MetadataMap -> Maybe (Map.Map String String)
extractErrorInfoMetadata meta = do
  statusBS <- lookupLast "grpc-status-details-bin" meta
  status <- eitherToMaybe $ fromByteString @Status statusBS
  errorInfoAny <- find (\any -> anyTypeUrl any == "type.googleapis.com/google.rpc.ErrorInfo") $ statusDetails status
  errorInfo <- eitherToMaybe $ fromByteString @ErrorInfo $ anyValue errorInfoAny
  pure $ Map.fromList $ fmap (bimap unpack unpack) $ Map.toList $ errorInfoMetadata errorInfo

unwrapWithExpectedFailures :: [StatusCode] -> ClientResult 'Normal a -> IO (Either (String, Maybe (Map.Map String String)) a)
unwrapWithExpectedFailures errs = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return $ Right x
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode code details meta))
        | code `elem` errs ->
          return $ Left (toString $ unStatusDetails details, extractErrorInfoMetadata meta)
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

unwrapWithNotFound :: ClientResult 'Normal a -> IO (Maybe a)
unwrapWithNotFound = fmap eitherToMaybe . unwrapWithExpectedFailures [StatusNotFound]

unwrapWithInvalidArgumentAndMetadata :: ClientResult 'Normal a -> IO (Either (String, Maybe (Map.Map String String)) a)
unwrapWithInvalidArgumentAndMetadata = unwrapWithExpectedFailures [StatusInvalidArgument]

unwrapWithInvalidArgument :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithInvalidArgument = fmap (first fst) . unwrapWithInvalidArgumentAndMetadata

unwrapWithCommandSubmissionFailure :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithCommandSubmissionFailure =
    fmap (first fst) . unwrapWithExpectedFailures [StatusInvalidArgument, StatusNotFound, StatusFailedPrecondition, StatusAlreadyExists]

unwrapWithTransactionFailures :: ClientResult 'Normal a -> IO (Either String a)
unwrapWithTransactionFailures =
    fmap (first fst) . unwrapWithExpectedFailures [StatusInvalidArgument, StatusNotFound]

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

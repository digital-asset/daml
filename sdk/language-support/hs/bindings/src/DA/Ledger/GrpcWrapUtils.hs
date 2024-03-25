-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Ledger.GrpcWrapUtils (
    unwrap, unwrapWithNotFound, unwrapWithInvalidArgument,
    unwrapWithCommandSubmissionFailure,
    unwrapWithTransactionFailures,
    ) where

import Prelude hiding (fail)

import Control.Exception (throwIO)
import Control.Monad.Fail (fail)
import Data.Either.Extra (eitherToMaybe)
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

-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandService (
    submitAndWait,
    submitAndWaitForTransactionId,
    submitAndWaitForTransaction,
    submitAndWaitForTransactionTree,
    ) where

import Data.Functor
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Com.Daml.Ledger.Api.V1.CommandService as LL

submitAndWait :: Commands -> LedgerService (Either String ())
submitAndWait commands =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionId=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithCommandSubmissionFailure
            <&> fmap (\LL.SubmitAndWaitForTransactionIdResponse{} -> ())

submitAndWaitForTransactionId :: Commands -> LedgerService (Either String TransactionId)
submitAndWaitForTransactionId commands =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionId=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithCommandSubmissionFailure
            <&> fmap (TransactionId . LL.submitAndWaitForTransactionIdResponseTransactionId)

submitAndWaitForTransaction :: Commands -> LedgerService (Either String Transaction)
submitAndWaitForTransaction commands =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransaction=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithCommandSubmissionFailure
            >>= \case
            Right response ->
                either (fail . show) (return . Right) $ raiseResponse response
            Left details ->
                return $ Left details
  where
      raiseResponse = \case
          LL.SubmitAndWaitForTransactionResponse{..} -> do
              perhaps "transaction" submitAndWaitForTransactionResponseTransaction
                  >>= raiseTransaction

submitAndWaitForTransactionTree :: Commands -> LedgerService (Either String TransactionTree)
submitAndWaitForTransactionTree commands =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionTree=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithCommandSubmissionFailure
            >>= \case
            Right response ->
                either (fail . show) (return . Right) $ raiseResponse response
            Left details ->
                return $ Left details
  where
      raiseResponse = \case
          LL.SubmitAndWaitForTransactionTreeResponse{..} -> do
              perhaps "transaction" submitAndWaitForTransactionTreeResponseTransaction
                  >>= raiseTransactionTree

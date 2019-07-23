-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandService (
    submitAndWait,
    submitAndWaitForTransactionId,
    submitAndWaitForTransaction,
    submitAndWaitForTransactionTree,
    ) where

import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Com.Digitalasset.Ledger.Api.V1.CommandService as LL

submitAndWait :: Commands -> LedgerService (Either String ())
submitAndWait commands =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionId=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands)) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse LL.SubmitAndWaitForTransactionIdResponse{} _m1 _m2 _status _details -> do
                return $ Right ()
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)

submitAndWaitForTransactionId :: Commands -> LedgerService (Either String TransactionId)
submitAndWaitForTransactionId commands =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionId=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands)) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse response _m1 _m2 _status _details -> do
                let LL.SubmitAndWaitForTransactionIdResponse{..} = response
                return $ Right $ TransactionId submitAndWaitForTransactionIdResponseTransactionId
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)

submitAndWaitForTransaction :: Commands -> LedgerService (Either String Transaction)
submitAndWaitForTransaction commands =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransaction=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands)) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse response _m1 _m2 _status _details -> do
                either (fail . show) (return . Right) $ raiseResponse response
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)
  where
      raiseResponse = \case
          LL.SubmitAndWaitForTransactionResponse{..} -> do
              perhaps "transaction" submitAndWaitForTransactionResponseTransaction
                  >>= raiseTransaction

submitAndWaitForTransactionTree :: Commands -> LedgerService (Either String TransactionTree)
submitAndWaitForTransactionTree commands =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.commandServiceClient client
        let LL.CommandService{commandServiceSubmitAndWaitForTransactionTree=rpc} = service
        let request = LL.SubmitAndWaitRequest (Just (lowerCommands commands)) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse response _m1 _m2 _status _details -> do
                either (fail . show) (return . Right) $ raiseResponse response
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)
  where
      raiseResponse = \case
          LL.SubmitAndWaitForTransactionTreeResponse{..} -> do
              perhaps "transaction" submitAndWaitForTransactionTreeResponseTransaction
                  >>= raiseTransactionTree

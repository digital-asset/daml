-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.TimeService (getTime,setTime) where

import Data.Functor
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.Convert
import qualified Com.Daml.Ledger.Api.V1.Testing.TimeService as LL
import Google.Protobuf.Empty (Empty(..))

getTime :: LedgerId -> LedgerService (Stream Timestamp)
getTime lid =
    makeLedgerService $ \timeout config mdm -> do
    let request = LL.GetTimeRequest (unLedgerId lid)
    asyncStreamGen $ \stream ->
        withGRPCClient config $ \client -> do
            service <- LL.timeServiceClient client
            let LL.TimeService {timeServiceGetTime=rpc} = service
            sendToStream timeout mdm request raiseGetTimeResponse stream rpc

-- | If the ledger responds with `StatusInvalidArgument`, we return `Left details` otherwise we return `Right ()`
setTime :: LedgerId -> Timestamp -> Timestamp -> LedgerService (Either String ())
setTime lid currentTime newTime  =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.timeServiceClient client
        let LL.TimeService {timeServiceSetTime=rpc} = service
        let request = LL.SetTimeRequest (unLedgerId lid) (Just (lowerTimestamp currentTime)) (Just (lowerTimestamp newTime))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithInvalidArgument
            <&> fmap (\Empty{} -> ())

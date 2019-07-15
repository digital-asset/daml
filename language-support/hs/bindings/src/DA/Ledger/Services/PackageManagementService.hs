-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageManagementService (uploadDarFile) where

import Com.Digitalasset.Ledger.Api.V1.Admin.PackageManagementService
import Data.ByteString(ByteString)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import Network.GRPC.HighLevel.Generated

-- | Upload a DAR file to the ledger. If the ledger responds with `INVALID_ARGUMENT`, we return `Left details`.
uploadDarFile :: ByteString -> LedgerService (Either String ()) -- Unlike other services, no LedgerId is needed. (why?!)
uploadDarFile bytes =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- packageManagementServiceClient client
        let PackageManagementService {packageManagementServiceUploadDarFile=rpc} = service
        let request = UploadDarFileRequest bytes
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse UploadDarFileResponse{} _m1 _m2 _status _details ->
                return $ Right ()
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)

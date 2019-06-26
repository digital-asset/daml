-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageService (
    listPackages,
    getPackage,
    getPackageStatus, PackageStatus(..),
    ) where

import Com.Digitalasset.Ledger.Api.V1.PackageService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import Proto3.Suite.Types(Enumerated(..))
import qualified DA.Daml.LF.Ast as LF(Package)
import qualified DA.Daml.LF.Proto3.Decode as Decode(decodePayload)
import qualified Data.Vector as Vector
import qualified Proto3.Suite(fromByteString)

listPackages :: LedgerId -> LedgerService [PackageId]
listPackages lid =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceListPackages=rpc} = service
        let request = ListPackagesRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        ListPackagesResponse xs <- unwrap response
        return $ map PackageId $ Vector.toList xs

getPackage :: LedgerId -> PackageId -> LedgerService (Maybe LF.Package)
getPackage lid pid =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackage=rpc} = service
        let request = GetPackageRequest (unLedgerId lid) (unPackageId pid) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _)) ->
                return Nothing
            ClientErrorResponse e ->
                fail (show e)
            ClientNormalResponse (GetPackageResponse _ bs _)  _m1 _m2 _status _details -> do
                let ap = either (error . show) id (Proto3.Suite.fromByteString bs)
                case Decode.decodePayload ap of
                    Left e -> fail (show e)
                    Right package -> return (Just package)

getPackageStatus :: LedgerId -> PackageId -> LedgerService PackageStatus
getPackageStatus lid pid =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackageStatus=rpc} = service
        let request = GetPackageStatusRequest (unLedgerId lid) (unPackageId pid) noTrace
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        unwrap response >>=
            \case
                GetPackageStatusResponse (Enumerated (Left n)) ->
                    fail $ "unexpected PackageStatus enum = " <> show n
                GetPackageStatusResponse (Enumerated (Right status)) ->
                    return status

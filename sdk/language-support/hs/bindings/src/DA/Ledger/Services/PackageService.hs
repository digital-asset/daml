-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageService (
    listPackages,
    getPackage, Package(..),
    getPackageStatus, PackageStatus(..),
    ) where

import Com.Daml.Ledger.Api.V1.PackageService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import Proto3.Suite.Types(Enumerated(..))
import qualified Data.Vector as Vector
import Data.ByteString(ByteString)

listPackages :: LedgerId -> LedgerService [PackageId]
listPackages lid =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceListPackages=rpc} = service
        let request = ListPackagesRequest (unLedgerId lid)
        response <- rpc (ClientNormalRequest request timeout mdm)
        ListPackagesResponse xs <- unwrap response
        return $ map PackageId $ Vector.toList xs

newtype Package = Package ByteString deriving (Eq,Ord,Show)

getPackage :: LedgerId -> PackageId -> LedgerService (Maybe Package)
getPackage lid pid =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackage=rpc} = service
        let request = GetPackageRequest (unLedgerId lid) (unPackageId pid)
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithNotFound
            >>= \case
            Nothing ->
                return Nothing
            Just (GetPackageResponse _ bs _) ->
                return $ Just $ Package bs

getPackageStatus :: LedgerId -> PackageId -> LedgerService PackageStatus
getPackageStatus lid pid =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- packageServiceClient client
        let PackageService {packageServiceGetPackageStatus=rpc} = service
        let request = GetPackageStatusRequest (unLedgerId lid) (unPackageId pid)
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            >>= \case
            GetPackageStatusResponse (Enumerated (Left n)) ->
                fail $ "unexpected PackageStatus enum = " <> show n
            GetPackageStatusResponse (Enumerated (Right status)) ->
                return status

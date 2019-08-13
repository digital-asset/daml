-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageManagementService (
    listKnownPackages, PackageDetails(..),
    uploadDarFile,
    ) where

import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Data.ByteString(ByteString)
import Data.Functor
import Data.Text.Lazy (Text)
import Network.GRPC.HighLevel.Generated
import qualified Com.Digitalasset.Ledger.Api.V1.Admin.PackageManagementService as LL

data PackageDetails = PackageDetails
    { pid :: PackageId
    , size :: Int
    , knownSince :: Timestamp
    , sourceDescription :: Text
    } deriving (Eq,Ord,Show)

listKnownPackages :: LedgerService [PackageDetails]
listKnownPackages =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.packageManagementServiceClient client
        let LL.PackageManagementService {packageManagementServiceListKnownPackages=rpc} = service
        let request = LL.ListKnownPackagesRequest
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= unwrap
            >>= either (fail . show) return . raiseResponse

raiseResponse ::  LL.ListKnownPackagesResponse -> Perhaps [PackageDetails]
raiseResponse = \case
    LL.ListKnownPackagesResponse{..} ->
        raiseList raisePackageDetails listKnownPackagesResponsePackageDetails

raisePackageDetails :: LL.PackageDetails -> Perhaps PackageDetails
raisePackageDetails = \case
    LL.PackageDetails{..} -> do
        pid <- raisePackageId packageDetailsPackageId
        let size = fromIntegral packageDetailsPackageSize
        knownSince <- perhaps "knownSince" packageDetailsKnownSince >>= raiseTimestamp
        let sourceDescription = packageDetailsSourceDescription
        return PackageDetails{..}

-- | Upload a DAR file to the ledger. If the ledger responds with `INVALID_ARGUMENT`, we return `Left details`.
uploadDarFile :: ByteString -> LedgerService (Either String ()) -- Unlike other services, no LedgerId is needed. (why?!)
uploadDarFile bytes =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- LL.packageManagementServiceClient client
        let LL.PackageManagementService {packageManagementServiceUploadDarFile=rpc} = service
        let request = LL.UploadDarFileRequest bytes
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= unwrapWithInvalidArgument
            <&> fmap (\LL.UploadDarFileResponse{} -> ())

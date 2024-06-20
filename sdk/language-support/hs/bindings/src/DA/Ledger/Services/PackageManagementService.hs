-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.PackageManagementService (
    listKnownPackages, PackageDetails(..),
    uploadDarFile,
    validateDarFile,
    ) where

import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Data.Bifunctor (bimap)
import Data.ByteString(ByteString)
import Data.Functor
import qualified Data.Map as Map
import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import Network.GRPC.HighLevel.Generated
import qualified Com.Daml.Ledger.Api.V1.Admin.PackageManagementService as LL

data PackageDetails = PackageDetails
    { pid :: PackageId
    , size :: Int
    , knownSince :: Timestamp
    , sourceDescription :: Text
    } deriving (Eq,Ord,Show)

listKnownPackages :: LedgerService [PackageDetails]
listKnownPackages =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.packageManagementServiceClient client
        let LL.PackageManagementService {packageManagementServiceListKnownPackages=rpc} = service
        let request = LL.ListKnownPackagesRequest
        rpc (ClientNormalRequest request timeout mdm)
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
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.packageManagementServiceClient client
        let LL.PackageManagementService {packageManagementServiceUploadDarFile=rpc} = service
        let request = LL.UploadDarFileRequest bytes TL.empty {- let server allocate submission id -}
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithInvalidArgumentAndMetadata
            <&> bimap collapseUploadError (\LL.UploadDarFileResponse{} -> ())

collapseUploadError :: (String, Maybe (Map.Map String String)) -> String
collapseUploadError (err, Just meta)
  | Just additionalInfo <- Map.lookup "additionalInfo" meta
  , Just upgradedPackage <- Map.lookup "upgradedPackage" meta
  , Just upgradingPackage <- Map.lookup "upgradingPackage" meta
  = err <> "\n  Reason: " <> additionalInfo <> "\n  When upgrading from package " <> upgradedPackage <> " to package " <> upgradingPackage
collapseUploadError (err, _) = err

-- | Validates a DAR file against the ledger. If the ledger responds with `INVALID_ARGUMENT`, we return `Left details`.
validateDarFile :: ByteString -> LedgerService (Either String ()) -- Unlike other services, no LedgerId is needed. (why?!)
validateDarFile bytes =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.packageManagementServiceClient client
        let LL.PackageManagementService {packageManagementServiceValidateDarFile=rpc} = service
        let request = LL.ValidateDarFileRequest bytes TL.empty {- let server allocate submission id -}
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithInvalidArgument
            <&> fmap (\LL.ValidateDarFileResponse{} -> ())

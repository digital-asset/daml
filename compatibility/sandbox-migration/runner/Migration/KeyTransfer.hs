-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE PatternSynonyms #-}
module Migration.KeyTransfer (test) where

import Control.Monad
import Control.Monad.Except
import qualified Data.Aeson as A
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types
import Migration.Util

test :: FilePath -> FilePath -> Test ([Tuple2 ContractId Asset], [Transaction]) Result
test step modelDar = Test {..}
  where
    initialState = ([], [])
    executeStep sdkVersion host port _state = withTempFile $ \outputFile -> do
        let suffix = getSdkVersion sdkVersion
        callProcess step
            [ "--host=" <> host
            , "--port=" <> show port
            , "--output", outputFile
            , "--dar=" <> modelDar
            , "--test=key-transfer," <> T.unpack (getParty testOwner) <> "," <> T.unpack (getParty testReceiver) <> "," <> suffix
            ]
        either fail pure =<< A.eitherDecodeFileStrict' outputFile
    validateStep sdkVersion (prevAssets, prevTransactions) Result {..} = do
        let suffix = getSdkVersion sdkVersion
        unless (equivalent oldAssets prevAssets) $
            throwError ("The old assets do not match those returned by the previous run: " <> show oldAssets)
        unless (oldTransactions == prevTransactions) $
            throwError ("The old transactions do not match those returned by the previous run: " <> show oldTransactions)
        let assetDiff = symDiff (map _2 oldAssets) (map _2 newAssets)
        unless (equivalent assetDiff [Asset testOwner ("keep-" <> suffix), Asset testReceiver ("transfer-" <> suffix)]) $
            throwError ("Expected one kept and one transferred contract, got " <> show assetDiff)
        let transactionDiff = symDiff (map events oldTransactions) (map events newTransactions)
        case transactionDiff of
            [[CreatedAsset (ContractId _) (Asset owner1 keep)],[CreatedAsset (ContractId archiveCreate) (Asset owner2 archive)],[ArchivedAsset (ContractId archiveArchive)],[CreatedAsset (ContractId transferCreate) (Asset owner3 transfer1)],[ArchivedAsset (ContractId transferArchive),CreatedAsset (ContractId _) (Asset receiver transfer2)]] -> do
                unless (archiveCreate == archiveArchive) $
                    throwError ("Mismatching contract idenfier between create and archive for asset " <> archive <> ", transaction diff: " <> show transactionDiff)
                let expectedTransfer = "transfer-" <> suffix
                unless (transfer1 == expectedTransfer && transfer2 == expectedTransfer) $
                    throwError ("Mismatching name for either asset '" <> transfer1 <> "' or '" <> transfer2 <> "' (expecting '" <> expectedTransfer <> "')), transaction diff: " <> show transactionDiff)
                unless (transferCreate == transferArchive) $
                    throwError ("Mismatching contract idenfier between create and archive for asset " <> expectedTransfer <> ", transaction diff: " <> show transactionDiff)
                let expectedArchive = "archive-" <> suffix
                unless (archive == expectedArchive) $
                    throwError ("Mismatching name for asset '" <> archive <> "' (expecting '" <> expectedArchive <> "')), transaction diff: " <> show transactionDiff)
                let expectedKeep = "keep-" <> suffix
                unless (keep == expectedKeep) $
                    throwError ("Mismatching name for asset '" <> keep <> "' (expecting '" <> expectedKeep <> "')), transaction diff: " <> show transactionDiff)
                unless (receiver == testReceiver) $
                    throwError ("Mismatching party '" <> show (getParty receiver) <> "' (expecting '" <> show (getParty testReceiver) <> "'), transaction diff: " <> show transactionDiff)
                unless (owner1 == testOwner) $
                    throwError ("Mismatching party '" <> show (getParty owner1) <> "' (expecting '" <> show (getParty testOwner) <> "'), transaction diff: " <> show transactionDiff)
                unless (owner2 == testOwner) $
                    throwError ("Mismatching party '" <> show (getParty owner2) <> "' (expecting '" <> show (getParty testOwner) <> "'), transaction diff: " <> show transactionDiff)
                unless (owner3 == testOwner) $
                    throwError ("Mismatching party '" <> show (getParty owner3) <> "' (expecting '" <> show (getParty testOwner) <> "'), transaction diff: " <> show transactionDiff)
            _ ->
                throwError ("Unexpected transaction pattern in " <> show transactionDiff)
        pure (newAssets, newTransactions)

    testOwner :: Party
    testOwner = Party "owner"
    testReceiver :: Party
    testReceiver = Party "receiver"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the Daml-LF JSON encoding.

pattern CreatedAsset :: ContractId -> Asset -> Event
pattern CreatedAsset cid asset <- Created cid (TemplateId "KeyTransfer" "Asset") (A.fromJSON -> A.Success asset)

pattern ArchivedAsset :: ContractId -> Event
pattern ArchivedAsset cid = Archived cid (TemplateId "KeyTransfer" "Asset")

data Asset = Asset
  { owner :: Party
  , name :: String
  } deriving (Eq, Generic, Show, Ord)

instance A.FromJSON Asset

data Result = Result
  { oldAssets :: [Tuple2 ContractId Asset]
  , newAssets :: [Tuple2 ContractId Asset]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
  } deriving Generic

instance A.FromJSON Result

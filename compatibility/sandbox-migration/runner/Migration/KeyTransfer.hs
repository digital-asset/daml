-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
module Migration.KeyTransfer (test) where

import qualified Data.Aeson as A
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types

test :: FilePath -> FilePath -> Test ([Tuple2 (ContractId Asset) Asset], [Transaction]) Result
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
    validateStep _sdkVersion prev Result{..} =
        Right prev

    testOwner :: Party
    testOwner = Party "owner"
    testReceiver :: Party
    testReceiver = Party "receiver"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the DAML-LF JSON encoding.

newtype ContractId t = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show)
newtype Party = Party { getParty :: T.Text }
  deriving newtype (A.FromJSON, A.ToJSON)
  deriving stock (Eq, Show)

data Asset = Asset
  { owner :: Party
  , name :: String
  } deriving (Eq, Generic, Show)

instance A.FromJSON Asset

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)

data Event
  = CreatedAsset (ContractId Asset) Asset
  | ArchivedAsset (ContractId Asset)
  deriving (Eq, Show)

instance A.FromJSON Event where
    parseJSON = A.withObject "Event" $ \o -> do
        ty <- o A..: "type"
        moduleName <- o A..: "moduleName"
        entityName <- o A..: "entityName"
        case moduleName of
            "KeyTransfer" -> case ty of
                "created" -> case entityName of
                    "Asset" -> CreatedAsset <$> o A..: "contractId" <*> o A..: "argument"
                    _ -> fail ("Invalid entity: " <> entityName)
                "archived" -> case entityName of
                    "Asset" -> ArchivedAsset <$> o A..: "contractId"
                    _ -> fail ("Invalid entity: " <> entityName)
                _ -> fail ("Invalid event type: " <> ty)
            _ -> fail ("Invalid module: " <> moduleName)

data Transaction = Transaction
  { transactionId :: T.Text
  , events :: [Event]
  } deriving (Generic, Eq, Show)

instance A.FromJSON Transaction

data Result = Result
  { oldKeptAssets :: [Tuple2 (ContractId Asset) Asset]
  , newKeptAssets :: [Tuple2 (ContractId Asset) Asset]
  , oldTransferredAssets :: [Tuple2 (ContractId Asset) Asset]
  , newTransferredAssets :: [Tuple2 (ContractId Asset) Asset]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
  } deriving Generic

instance A.FromJSON Result

-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
module Migration.KeyTransfer (test) where

import Control.Monad
import Control.Monad.Except
import qualified Data.Aeson as A
import Data.List
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types

diff :: Eq a => [a] -> [a] -> [a]
diff left right = (left \\ right) ++ (right \\ left)

equivalent :: Eq a => [a] -> [a] -> Bool
equivalent xs = null . diff xs

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
    validateStep sdkVersion (prevAssets, prevTransactions) Result {..} = do
        let suffix = getSdkVersion sdkVersion
        unless (equivalent oldAssets prevAssets) $
            throwError ("The old assets do not match those returned by the previous run: " <> show oldAssets)
        unless (equivalent oldTransactions prevTransactions) $
            throwError ("The old transactions do not match those returned by the previous run: " <> show oldTransactions)
        let assetDiff = diff (map _2 oldAssets) (map _2 newAssets)
        unless (equivalent assetDiff [Asset testOwner ("keep-" <> suffix), Asset testReceiver ("transfer-" <> suffix)]) $
            throwError ("Expected one kept and one transferred contract, got " <> show assetDiff)
        let transactionDiff = concat $ diff (map events oldTransactions) (map events newTransactions)
        unless (length transactionDiff == 6) $
            throwError ("Expected six unique contract state changes, transaction diff: " <> show transactionDiff)
        let groupedByContract = groupBy contractId transactionDiff
        unless (length groupedByContract == 4) $
            throwError ("Expected four unique contract idenfiers, transaction diff: " <> show transactionDiff)
        unless (any (activeContract testOwner ("keep-" <> suffix)) groupedByContract) $
            throwError ("Cannot find create event for active kept contract, transaction diff: " <> show transactionDiff)
        unless (any (archivedContract testOwner ("archive-" <> suffix)) groupedByContract) $
            throwError ("Cannot find create event for archived contract, transaction diff: " <> show transactionDiff)
        unless (any (archivedContract testOwner ("transfer-" <> suffix)) groupedByContract) $
            throwError ("Cannot find create event for archived transferred contract, transaction diff: " <> show transactionDiff)
        unless (any (activeContract testReceiver ("transfer-" <> suffix)) groupedByContract) $
            throwError ("Cannot find create event for active transferred contract, transaction diff: " <> show transactionDiff)
        pure (newAssets, newTransactions)

    testOwner :: Party
    testOwner = Party "owner"
    testReceiver :: Party
    testReceiver = Party "receiver"

activeContract :: Party -> String -> [Event] -> Bool
activeContract expectedParty expectedName event =
    case event of
        [CreatedAsset _ (Asset party name)] -> party == expectedParty && name == expectedName
        _ -> False

archivedContract :: Party -> String -> [Event] -> Bool
archivedContract expectedParty expectedName event =
    case event of
        [CreatedAsset _ (Asset party name), ArchivedAsset _] -> party == expectedParty && name == expectedName
        _ -> False

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

getContractId :: Event -> T.Text
getContractId event =
    case event of
        CreatedAsset (ContractId cid) _ -> cid
        ArchivedAsset (ContractId cid) -> cid

contractId :: Event -> Event -> Bool
contractId e1 e2 = getContractId e1 == getContractId e2

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
  { oldAssets :: [Tuple2 (ContractId Asset) Asset]
  , newAssets :: [Tuple2 (ContractId Asset) Asset]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
  } deriving Generic

instance A.FromJSON Result

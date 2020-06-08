-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
module Migration.ProposeAccept (test) where

import Control.Monad
import Control.Monad.Except
import qualified Data.Aeson as A
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types

test :: FilePath -> FilePath -> Test ([Tuple2 (ContractId Deal) Deal], [Transaction]) Result
test step modelDar = Test {..}
  where
    initialState = ([], [])
    executeStep sdkVersion host port _state = withTempFile $ \outputFile -> do
        let note = getSdkVersion sdkVersion
        callProcess step
            [ "--output", outputFile
            , "--host=" <> host
            , "--port=" <> show port
            , "--proposer=" <> T.unpack (getParty testProposer)
            , "--accepter=" <> T.unpack (getParty testAccepter)
            , "--note=" <> note
            , "--dar=" <> modelDar
            ]
        either fail pure =<< A.eitherDecodeFileStrict' outputFile
    validateStep sdkVersion (prevTs, prevTransactions) Result{..} = do
        let note = getSdkVersion sdkVersion
        -- Test that all proposals are archived.
        unless (null oldProposeDeals) $
            throwError ("Expected no old ProposeDeals but got " <> show oldProposeDeals)
        unless (null newProposeDeals) $
            throwError ("Expected no new ProposeDeals but got " <> show newProposeDeals)
        unless (prevTs == oldDeals) $
            throwError ("Active ts should not have changed after migration: " <> show (prevTs, oldDeals))
        -- Test that no T contracts got archived.
        let missingTs = filter (`notElem` newDeals) oldDeals
        unless (null missingTs) $
            throwError ("The following contracts got lost during the migration: " <> show missingTs)
        -- Test that only one new T contract is not archived.
        let addedTs = filter (`notElem` oldDeals) newDeals
        case addedTs of
            [Tuple2 _ t] -> do
                let expected = Deal testProposer testAccepter note
                unless (t == expected) $
                    throwError ("Expected " <> show expected <> " but got " <> show t)
            _ -> throwError ("Expected 1 new T contract but got: " <> show addedTs)
        -- verify that the stream before and after the migration are the same.
        unless (prevTransactions == oldTransactions) $
            fail $ "Transaction stream changed after migration "
                <> show (prevTransactions, oldTransactions)
        -- verify that we created the right number of new transactions.
        unless (length newTransactions == length oldTransactions + 5) $
            fail $ "Expected 3 new transactions but got "
                <> show (length newTransactions - length oldTransactions)
        let (newStart, newEnd) = splitAt (length oldTransactions) newTransactions
        -- verify that the new stream is identical to the old if we cut off the new transactions.
        unless (newStart == oldTransactions) $
            fail $ "New transaction stream does not contain old transactions "
                <> show (oldTransactions, newStart)
        -- verify that the new transactions are what we expect.
        validateNewTransactions (map events newEnd)
        pure (newDeals, newTransactions)
    validateNewTransactions :: [[Event]] -> Either String ()
    validateNewTransactions = \case
      [ [CreatedProposeDeal prop1 _],
        [CreatedProposeDeal prop2 _],
        [ArchivedProposeDeal prop1',CreatedDeal t1 _],
        [ArchivedProposeDeal prop2',CreatedDeal _t2 _],
        [ArchivedDeal t1'] ] -> do
        checkArchive prop1 prop1'
        checkArchive prop2 prop2'
        checkArchive t1 t1'
      events -> throwError ("Unexpected events: " <> show events)
    checkArchive cid cid' = unless (cid == cid') $
        throwError ("Expected " <> show cid <> " to be archived but got " <> show cid')

    testProposer :: Party
    testProposer = Party "proposer"
    testAccepter :: Party
    testAccepter = Party "accepter"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the DAML-LF JSON encoding.

newtype ContractId t = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show)
newtype Party = Party { getParty :: T.Text }
  deriving newtype (A.FromJSON, A.ToJSON)
  deriving stock (Eq, Show)

data Deal = Deal
  { proposer :: Party
  , accepter :: Party
  , note :: String
  } deriving (Eq, Generic, Show)

instance A.FromJSON Deal

data ProposeDeal = ProposeDeal
  { proposer :: Party
  , accepter :: Party
  , note :: T.Text
  } deriving (Generic, Show, Eq)

instance A.FromJSON ProposeDeal

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)

data Event
  = CreatedDeal (ContractId Deal) Deal
  | ArchivedDeal (ContractId Deal)
  | CreatedProposeDeal (ContractId ProposeDeal) ProposeDeal
  | ArchivedProposeDeal (ContractId ProposeDeal)
  deriving (Eq, Show)

instance A.FromJSON Event where
    parseJSON = A.withObject "Event" $ \o -> do
        ty <- o A..: "type"
        moduleName <- o A..: "moduleName"
        entityName <- o A..: "entityName"
        case moduleName of
            "ProposeAccept" -> case ty of
                "created" -> case entityName of
                    "Deal" -> CreatedDeal <$> o A..: "contractId" <*> o A..: "argument"
                    "ProposeDeal" -> CreatedProposeDeal <$> o A..: "contractId" <*> o A..: "argument"
                    _ -> fail ("Invalid entity: " <> entityName)
                "archived" -> case entityName of
                    "Deal" -> ArchivedDeal <$> o A..: "contractId"
                    "ProposeDeal" -> ArchivedProposeDeal <$> o A..: "contractId"
                    _ -> fail ("Invalid entity: " <> entityName)
                _ -> fail ("Invalid event type: " <> ty)
            _ -> fail ("Invalid module: " <> moduleName)

data Transaction = Transaction
  { transactionId :: T.Text
  , events :: [Event]
  } deriving (Generic, Eq, Show)

instance A.FromJSON Transaction

data Result = Result
  { oldProposeDeals :: [Tuple2 (ContractId ProposeDeal) ProposeDeal]
  , newProposeDeals :: [Tuple2 (ContractId ProposeDeal) ProposeDeal]
  , oldDeals :: [Tuple2 (ContractId Deal) Deal]
  , newDeals :: [Tuple2 (ContractId Deal) Deal]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
  } deriving Generic

instance A.FromJSON Result


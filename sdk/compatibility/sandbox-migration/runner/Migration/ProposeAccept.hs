-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE PatternSynonyms #-}
module Migration.ProposeAccept (test) where

import Control.Monad
import Control.Monad.Except
import qualified Data.Aeson as A
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types

test :: FilePath -> FilePath -> Test ([Tuple2 ContractId Deal], [Transaction]) Result
test step modelDar = Test {..}
  where
    initialState = ([], [])
    executeStep sdkVersion host port _state = withTempFile $ \outputFile -> do
        let note = getSdkVersion sdkVersion
        callProcess step
            [ "--host=" <> host
            , "--port=" <> show port
            , "--output", outputFile
            , "--dar=" <> modelDar
            , "--test=propose-accept," <> T.unpack (getParty testProposer) <> "," <> T.unpack (getParty testAccepter) <> "," <> note
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
            Left $ "Transaction stream changed after migration "
                <> show (prevTransactions, oldTransactions)
        -- verify that we created the right number of new transactions.
        unless (length newTransactions == length oldTransactions + 5) $
            Left $ "Expected 3 new transactions but got "
                <> show (length newTransactions - length oldTransactions)
        let (newStart, newEnd) = splitAt (length oldTransactions) newTransactions
        -- verify that the new stream is identical to the old if we cut off the new transactions.
        unless (newStart == oldTransactions) $
            Left $ "New transaction stream does not contain old transactions "
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
-- match the Daml-LF JSON encoding.

data Deal = Deal
  { proposer :: Party
  , accepter :: Party
  , note :: String
  } deriving (Eq, Generic, Show, Ord)

instance A.FromJSON Deal

data ProposeDeal = ProposeDeal
  { proposer :: Party
  , accepter :: Party
  , note :: T.Text
  } deriving (Generic, Show, Eq)

instance A.FromJSON ProposeDeal

data Result = Result
  { oldProposeDeals :: [Tuple2 ContractId ProposeDeal]
  , newProposeDeals :: [Tuple2 ContractId ProposeDeal]
  , oldDeals :: [Tuple2 ContractId Deal]
  , newDeals :: [Tuple2 ContractId Deal]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
  } deriving Generic

instance A.FromJSON Result


pattern CreatedProposeDeal :: ContractId -> ProposeDeal -> Event
pattern CreatedProposeDeal cid asset <- Created cid (TemplateId "ProposeAccept" "ProposeDeal") (A.fromJSON -> A.Success asset)

pattern ArchivedProposeDeal :: ContractId -> Event
pattern ArchivedProposeDeal cid = Archived cid (TemplateId "ProposeAccept" "ProposeDeal")

pattern CreatedDeal :: ContractId -> ProposeDeal -> Event
pattern CreatedDeal cid asset <- Created cid (TemplateId "ProposeAccept" "Deal") (A.fromJSON -> A.Success asset)

pattern ArchivedDeal :: ContractId -> Event
pattern ArchivedDeal cid <- Archived cid (TemplateId "ProposeAccept" "Deal")

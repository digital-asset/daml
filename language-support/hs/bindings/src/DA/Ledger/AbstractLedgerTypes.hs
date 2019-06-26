-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.AbstractLedgerTypes (
    LedgerOffset, offsetBegin,
    TransactionFilter, filterEverthingForParty
    ) where

import Com.Digitalasset.Ledger.Api.V1.LedgerOffset
import Com.Digitalasset.Ledger.Api.V1.TransactionFilter
import DA.Ledger.Types
import Proto3.Suite.Types
import qualified Data.Map as Map

offsetBegin :: LedgerOffset
offsetBegin = LedgerOffset {
    ledgerOffsetValue = Just (LedgerOffsetValueBoundary (Enumerated (Right boundaryBegin)))
    }

boundaryBegin :: LedgerOffset_LedgerBoundary
boundaryBegin = LedgerOffset_LedgerBoundaryLEDGER_BEGIN

filterEverthingForParty :: Party -> TransactionFilter
filterEverthingForParty party = TransactionFilter (Map.singleton (unParty party) (Just noFilters))

noFilters :: Filters
noFilters = Filters Nothing

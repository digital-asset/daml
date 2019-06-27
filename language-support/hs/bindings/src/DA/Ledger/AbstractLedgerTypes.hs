-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.AbstractLedgerTypes (
    LedgerOffset, offsetBegin, mkAbsLedgerOffset,
    TransactionFilter, filterEverthingForParty
    ) where

import Com.Digitalasset.Ledger.Api.V1.LedgerOffset
import Com.Digitalasset.Ledger.Api.V1.TransactionFilter
import DA.Ledger.Types
import Proto3.Suite.Types
import qualified Data.Map as Map

-- TODO: Make LedgerOffset a proper wrapped type in Types.hs

mkAbsLedgerOffset :: AbsOffset -> LedgerOffset
mkAbsLedgerOffset abs = LedgerOffset {
    ledgerOffsetValue = Just (LedgerOffsetValueAbsolute (unAbsOffset abs))
    }

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

-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Transaction( -- WIP. highlevel definition, decoupled from the generated code
    Transaction,
    fromServiceResponse,
    ) where  

import qualified Data.Vector        as Vector
import qualified Data.Text.Lazy     as Text

import Com.Digitalasset.Ledger.Api.V1.TransactionService (GetTransactionsResponse(..))

import qualified Com.Digitalasset.Ledger.Api.V1.Transaction as LLT

newtype Transaction = Transaction { low :: LLT.Transaction }

instance Show Transaction where
    show Transaction{low} = _summary --_full
        where
            _summary = "Trans:id=" <> Text.unpack transactionTransactionId
            _full = show low
            LLT.Transaction{transactionTransactionId} = low

fromServiceResponse :: GetTransactionsResponse -> [Transaction]
fromServiceResponse GetTransactionsResponse{getTransactionsResponseTransactions = ts} =
    map Transaction (Vector.toList ts)

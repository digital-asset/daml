// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.ledger.api.domain.{LedgerId, LedgerOffset, TransactionFilter}

final case class GetTransactionsRequest(
    ledgerId: Option[LedgerId],
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    filter: TransactionFilter,
    verbose: Boolean,
)

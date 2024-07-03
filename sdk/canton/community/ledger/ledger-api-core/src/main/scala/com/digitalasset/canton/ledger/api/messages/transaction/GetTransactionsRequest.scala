// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset, TransactionFilter}

final case class GetTransactionsRequest(
    ledgerId: Option[LedgerId],
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    filter: TransactionFilter,
    sendPrunedOffsets: Boolean,
    verbose: Boolean,
)

// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.ledger.api.domain.{LedgerId, LedgerOffset, TransactionFilter}

final case class GetTransactionsRequest(
    ledgerId: LedgerId,
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    filter: TransactionFilter,
    verbose: Boolean,
    traceContext: Option[TraceContext])

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.ledger.api.domain.{LedgerOffset, TransactionFilter}

final case class GetTransactionsRequest(
    ledgerId: LedgerId,
    begin: LedgerOffset,
    end: Option[LedgerOffset],
    filter: TransactionFilter,
    verbose: Boolean,
    traceContext: Option[TraceContext])

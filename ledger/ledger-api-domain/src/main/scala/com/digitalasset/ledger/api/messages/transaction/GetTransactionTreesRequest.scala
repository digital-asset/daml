// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}

import scala.collection.immutable

final case class GetTransactionTreesRequest(
    ledgerId: LedgerId,
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    parties: immutable.Set[Party],
    verbose: Boolean,
    traceContext: Option[TraceContext])

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref.{LedgerId, Party}
import com.digitalasset.ledger.api.domain.LedgerOffset

import scala.collection.immutable

final case class GetTransactionTreesRequest(
    ledgerId: LedgerId,
    begin: LedgerOffset,
    end: Option[LedgerOffset],
    parties: immutable.Set[Party],
    verbose: Boolean,
    traceContext: Option[TraceContext])

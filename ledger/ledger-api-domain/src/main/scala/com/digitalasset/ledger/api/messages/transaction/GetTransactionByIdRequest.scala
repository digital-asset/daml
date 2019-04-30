// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain.{LedgerId, TransactionId}

import scala.collection.immutable

final case class GetTransactionByIdRequest(
    ledgerId: LedgerId,
    transactionId: TransactionId,
    requestingParties: immutable.Set[Party],
    returnFlatTransaction: Boolean,
    traceContext: Option[TraceContext])

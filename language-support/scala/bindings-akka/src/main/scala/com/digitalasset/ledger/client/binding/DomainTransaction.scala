// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.refinements.ApiTypes.{CommandId, TransactionId, WorkflowId}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.google.protobuf.timestamp.Timestamp

case class DomainTransaction(
    transactionId: TransactionId,
    workflowId: WorkflowId,
    offset: LedgerOffset,
    commandId: CommandId,
    effectiveAt: Timestamp,
    events: Seq[DomainEvent],
    traceContext: Option[TraceContext])

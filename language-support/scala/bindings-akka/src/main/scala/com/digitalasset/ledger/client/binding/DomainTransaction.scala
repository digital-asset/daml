// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes.{CommandId, TransactionId, WorkflowId}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.google.protobuf.timestamp.Timestamp

case class DomainTransaction(
    transactionId: TransactionId,
    workflowId: WorkflowId,
    offset: LedgerOffset,
    commandId: CommandId,
    effectiveAt: Timestamp,
    events: Seq[DomainEvent],
)

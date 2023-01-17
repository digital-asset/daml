// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.tracing.SpanAttribute

/** Extracts identifiers from Protobuf messages to correlate traces.
  */
object TraceIdentifiers {

  /** Extract identifiers from a transaction message.
    */
  def fromTransaction(transaction: Transaction): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]
    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (!value.isEmpty) attributes += attribute -> value

    setIfNotEmpty(SpanAttribute.Offset, transaction.offset)
    setIfNotEmpty(SpanAttribute.CommandId, transaction.commandId)
    setIfNotEmpty(SpanAttribute.TransactionId, transaction.transactionId)
    setIfNotEmpty(SpanAttribute.WorkflowId, transaction.workflowId)

    attributes.result()
  }

  /** Extract identifiers from a transaction tree message.
    */
  def fromTransactionTree(transactionTree: TransactionTree): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]
    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (!value.isEmpty) attributes += attribute -> value

    setIfNotEmpty(SpanAttribute.Offset, transactionTree.offset)
    setIfNotEmpty(SpanAttribute.CommandId, transactionTree.commandId)
    setIfNotEmpty(SpanAttribute.TransactionId, transactionTree.transactionId)
    setIfNotEmpty(SpanAttribute.WorkflowId, transactionTree.workflowId)

    attributes.result()
  }
}

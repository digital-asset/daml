// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.{Transaction, TransactionTree}
import com.daml.tracing.SpanAttribute

/** Extracts identifiers from Protobuf messages to correlate traces.
  */
object TraceIdentifiers {

  /** Extract identifiers from a transaction message.
    */
  def fromTransaction(transaction: Transaction): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]
    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (value.nonEmpty) attributes += attribute -> value
    def setIfNotZero(attribute: SpanAttribute, value: Long): Unit =
      if (value != 0) attributes += attribute -> value.toString

    setIfNotZero(SpanAttribute.Offset, transaction.offset)
    setIfNotEmpty(SpanAttribute.CommandId, transaction.commandId)
    setIfNotEmpty(SpanAttribute.TransactionId, transaction.updateId)
    setIfNotEmpty(SpanAttribute.WorkflowId, transaction.workflowId)

    attributes.result()
  }

  /** Extract identifiers from a transaction tree message.
    */
  def fromTransactionTree(transactionTree: TransactionTree): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]
    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (value.nonEmpty) attributes += attribute -> value
    def setIfNotZero(attribute: SpanAttribute, value: Long): Unit =
      if (value != 0) attributes += attribute -> value.toString

    setIfNotZero(SpanAttribute.Offset, transactionTree.offset)
    setIfNotEmpty(SpanAttribute.CommandId, transactionTree.commandId)
    setIfNotEmpty(SpanAttribute.TransactionId, transactionTree.updateId)
    setIfNotEmpty(SpanAttribute.WorkflowId, transactionTree.workflowId)

    attributes.result()
  }

  /** Extract identifiers from a reassignment message.
    */
  def fromReassignment(reassignment: Reassignment): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (value.nonEmpty) attributes += attribute -> value
    def setIfNotZero(attribute: SpanAttribute, value: Long): Unit =
      if (value != 0) attributes += attribute -> value.toString

    setIfNotZero(SpanAttribute.Offset, reassignment.offset)
    setIfNotEmpty(SpanAttribute.CommandId, reassignment.commandId)
    setIfNotEmpty(SpanAttribute.TransactionId, reassignment.updateId)
    setIfNotEmpty(SpanAttribute.WorkflowId, reassignment.workflowId)

    attributes.result()
  }

  def fromTopologyTransaction(
      topologyTransaction: TopologyTransaction
  ): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    def setIfNotEmpty(attribute: SpanAttribute, value: String): Unit =
      if (value.nonEmpty) attributes += attribute -> value
    def setIfNotZero(attribute: SpanAttribute, value: Long): Unit =
      if (value != 0) attributes += attribute -> value.toString

    setIfNotZero(SpanAttribute.Offset, topologyTransaction.offset)
    setIfNotEmpty(SpanAttribute.TransactionId, topologyTransaction.updateId)

    attributes.result()
  }
}

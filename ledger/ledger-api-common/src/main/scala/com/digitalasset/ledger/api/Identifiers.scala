// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger._
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}

/**
  * Extracts identifiers from Protobuf messages to correlate traces.
  */
object Identifiers {

  /**
    * Extract identifiers from a transaction message.
    */
  def fromTransaction(transaction: Transaction): Map[String, String] = {
    val attributes = Map.newBuilder[String, String]
    if (!transaction.offset.isEmpty) attributes += OffsetKey -> transaction.offset
    if (!transaction.commandId.isEmpty) attributes += CommandIdKey -> transaction.commandId
    if (!transaction.transactionId.isEmpty)
      attributes += TransactionIdKey -> transaction.transactionId
    if (!transaction.workflowId.isEmpty) attributes += WorkflowIdKey -> transaction.workflowId
    attributes.result
  }

  /**
    * Extract identifiers from a transaction tree message.
    */
  def fromTransactionTree(transactionTree: TransactionTree): Map[String, String] = {
    val attributes = Map.newBuilder[String, String]
    if (!transactionTree.offset.isEmpty) attributes += OffsetKey -> transactionTree.offset
    if (!transactionTree.commandId.isEmpty) attributes += CommandIdKey -> transactionTree.commandId
    if (!transactionTree.transactionId.isEmpty)
      attributes += TransactionIdKey -> transactionTree.transactionId
    if (!transactionTree.workflowId.isEmpty)
      attributes += WorkflowIdKey -> transactionTree.workflowId
    attributes.result
  }
}

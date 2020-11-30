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
    def setIfNotEmpty(key: String, value: String): Unit =
      if (!value.isEmpty) attributes += key -> value

    setIfNotEmpty(OffsetKey, transaction.offset)
    setIfNotEmpty(CommandIdKey, transaction.commandId)
    setIfNotEmpty(TransactionIdKey, transaction.transactionId)
    setIfNotEmpty(WorkflowIdKey, transaction.workflowId)

    attributes.result
  }

  /**
    * Extract identifiers from a transaction tree message.
    */
  def fromTransactionTree(transactionTree: TransactionTree): Map[String, String] = {
    val attributes = Map.newBuilder[String, String]
    def setIfNotEmpty(key: String, value: String): Unit =
      if (!value.isEmpty) attributes += key -> value

    setIfNotEmpty(OffsetKey, transactionTree.offset)
    setIfNotEmpty(CommandIdKey, transactionTree.commandId)
    setIfNotEmpty(TransactionIdKey, transactionTree.transactionId)
    setIfNotEmpty(WorkflowIdKey, transactionTree.workflowId)

    attributes.result
  }
}

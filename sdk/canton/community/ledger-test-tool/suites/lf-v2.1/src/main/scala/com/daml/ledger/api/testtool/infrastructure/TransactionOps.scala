// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction.Transaction.toJavaProto
import com.daml.ledger.javaapi.data.Transaction as JavaTransaction

import scala.jdk.CollectionConverters.*

object TransactionOps {

  implicit class TransactionOps(val tx: Transaction) extends AnyVal {

    def rootNodeIds(): List[Int] =
      JavaTransaction.fromProto(toJavaProto(tx)).getRootNodeIds.asScala.map(_.toInt).toList

  }
}

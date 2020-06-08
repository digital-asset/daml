// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.events

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Transaction
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class TransactionIdWithIndexSpec extends WordSpec with Matchers with ScalaFutures {

  "TransactionIdWithIndex" should {
    val transactionId: Ref.LedgerString = Ref.LedgerString.fromInt(42)
    val index: Transaction.NodeId = Transaction.NodeId(42)
    val referenceEventID = s"#$transactionId:${index.index}"

    "format an EventId from a TransactionId and an index" in {
      TransactionIdWithIndex(transactionId, index).toLedgerString shouldBe referenceEventID
    }

    "split an eventId into a transactionId and an index" in {
      TransactionIdWithIndex.fromString(referenceEventID) shouldBe Right(
        TransactionIdWithIndex(transactionId, index))
    }

    "return None when parsing an invalid argument" in {
      TransactionIdWithIndex.fromString("some invalid event id") shouldBe 'left
    }
  }
}

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import com.digitalasset.daml.lf.transaction.Transaction
import SandboxEventIdFormatter.TransactionIdWithIndex
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class EventIdFormatterSpec extends WordSpec with Matchers with ScalaFutures {

  "EventIdFormatter" should {
    val transactionId: Ref.TransactionId = Ref.LedgerName.assertFromString("42")
    val index: Transaction.NodeId = Transaction.NodeId.unsafeFromIndex(42)
    val referenceEventID = s"#$transactionId:${index.index}"

    "format an EventId from a TransactionId and an index" in {
      SandboxEventIdFormatter.fromTransactionId(transactionId, index) should equal(referenceEventID)
    }

    "split an eventId into a transactionId and an index" in {
      SandboxEventIdFormatter.split(referenceEventID) should equal(
        Some(TransactionIdWithIndex(transactionId, index)))
    }

    "return None when parsing an invalid argument" in {
      SandboxEventIdFormatter.split("some invalid event id") should equal(None)
    }
  }
}

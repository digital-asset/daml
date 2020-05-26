// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.events

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Transaction
import com.daml.platform.events.EventIdFormatter.TransactionIdWithIndex
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class EventIdFormatterSpec extends WordSpec with Matchers with ScalaFutures {

  "EventIdFormatter" should {
    val transactionId: Ref.LedgerString = Ref.LedgerString.fromInt(42)
    val index: Transaction.NodeId = Transaction.NodeId(42)
    val referenceEventID = s"#$transactionId:${index.index}"

    "format an EventId from a TransactionId and an index" in {
      EventIdFormatter.fromTransactionId(transactionId, index) should equal(referenceEventID)
    }

    "split an eventId into a transactionId and an index" in {
      EventIdFormatter.split(referenceEventID) should equal(
        Some(TransactionIdWithIndex(transactionId, index)))
    }

    "return None when parsing an invalid argument" in {
      EventIdFormatter.split("some invalid event id") should equal(None)
    }
  }
}

// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.platform.sandbox.SandboxEventIdFormatter.TransactionIdWithIndex
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class SandboxEventIdFormatterSpec extends WordSpec with Matchers with ScalaFutures {

  "SandboxEventIdFormatter" should {
    val transactionId: Ref.TransactionIdString = Ref.TransactionIdString.fromInt(42)
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

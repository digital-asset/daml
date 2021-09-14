// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.offset.Offset
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.status.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CompletionFromTransactionSpec extends AnyWordSpec with Matchers {

  "CompletionFromTransaction" should {
    "acceptedCompletion" in {
      val completionStream = CompletionFromTransaction.acceptedCompletion(
        Instant.EPOCH,
        Offset.beforeBegin,
        "commandId",
        "transactionId",
        "applicationId",
        Some("submissionId"),
      )

      val checkpoint = completionStream.checkpoint.get
      checkpoint.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
      checkpoint.offset shouldBe a[Some[_]]

      val completion = completionStream.completions.head
      completion.commandId shouldBe "commandId"
      completion.transactionId shouldBe "transactionId"
      completion.applicationId shouldBe "applicationId"
      completion.submissionId shouldBe "submissionId"
    }

    "rejectedCompletion" in {
      val status = Status.of(io.grpc.Status.Code.INTERNAL.value(), "message", Seq.empty)
      val completionStream = CompletionFromTransaction.rejectedCompletion(
        Instant.EPOCH,
        Offset.beforeBegin,
        "commandId",
        status,
        "applicationId",
        Some("submissionId"),
      )

      val checkpoint = completionStream.checkpoint.get
      checkpoint.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
      checkpoint.offset shouldBe a[Some[_]]

      val completion = completionStream.completions.head
      completion.commandId shouldBe "commandId"
      completion.applicationId shouldBe "applicationId"
      completion.submissionId shouldBe "submissionId"
      completion.status shouldBe Some(status)
    }
  }
}

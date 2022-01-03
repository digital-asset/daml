// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.api.v1.completion.Completion.DeduplicationPeriod
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.status.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CompletionFromTransactionSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks {

  "CompletionFromTransaction" should {
    "create an accepted completion" in {
      val testCases = Table(
        (
          "submissionId",
          "deduplicationOffset",
          "deduplicationDurationSeconds",
          "deduplicationDurationNanos",
          "expectedSubmissionId",
          "expectedDeduplicationPeriod",
        ),
        (Some("submissionId"), None, None, None, "submissionId", DeduplicationPeriod.Empty),
        (None, None, None, None, "", DeduplicationPeriod.Empty),
        (None, Some("offset"), None, None, "", DeduplicationPeriod.DeduplicationOffset("offset")),
        (
          None,
          None,
          Some(1L),
          Some(2),
          "",
          DeduplicationPeriod
            .DeduplicationDuration(new Duration(1, 2))
            .asInstanceOf[ // otherwise the compilation fails due to an inference warning
              DeduplicationPeriod
            ],
        ),
      )

      forEvery(testCases) {
        (
            submissionId,
            deduplicationOffset,
            deduplicationDurationSeconds,
            deduplicationDurationNanos,
            expectedSubmissionId,
            expectedDeduplicationPeriod,
        ) =>
          val completionStream = CompletionFromTransaction.acceptedCompletion(
            Time.Timestamp.Epoch,
            Offset.beforeBegin,
            "commandId",
            "transactionId",
            "applicationId",
            submissionId,
            deduplicationOffset,
            deduplicationDurationSeconds,
            deduplicationDurationNanos,
          )

          val checkpoint = completionStream.checkpoint.get
          checkpoint.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
          checkpoint.offset shouldBe a[Some[_]]

          val completion = completionStream.completions.head
          completion.commandId shouldBe "commandId"
          completion.transactionId shouldBe "transactionId"
          completion.applicationId shouldBe "applicationId"
          completion.submissionId shouldBe expectedSubmissionId
          completion.deduplicationPeriod shouldBe expectedDeduplicationPeriod
      }
    }

    "fail on an invalid deduplication duration" in {
      val testCases = Table(
        ("deduplicationDurationSeconds", "deduplicationDurationNanos"),
        (Some(1L), None),
        (None, Some(1)),
      )

      forEvery(testCases) { (deduplicationDurationSeconds, deduplicationDurationNanos) =>
        an[IllegalArgumentException] shouldBe thrownBy(
          CompletionFromTransaction.acceptedCompletion(
            Time.Timestamp.Epoch,
            Offset.beforeBegin,
            "commandId",
            "transactionId",
            "applicationId",
            Some("submissionId"),
            None,
            deduplicationDurationSeconds,
            deduplicationDurationNanos,
          )
        )
      }
    }

    "create a rejected completion" in {
      val status = Status.of(io.grpc.Status.Code.INTERNAL.value(), "message", Seq.empty)
      val completionStream = CompletionFromTransaction.rejectedCompletion(
        Time.Timestamp.Epoch,
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

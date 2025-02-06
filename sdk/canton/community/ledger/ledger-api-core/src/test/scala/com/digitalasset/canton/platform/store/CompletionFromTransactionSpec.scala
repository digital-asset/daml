// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.api.v2.completion.Completion.DeduplicationPeriod
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.Offset
import com.digitalasset.daml.lf.data.Time
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.status.Status
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CompletionFromTransactionSpec
    extends AnyWordSpec
    with Matchers
    with OptionValues
    with TestEssentials
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
        (
          None,
          Some(12345678L),
          None,
          None,
          "",
          DeduplicationPeriod.DeduplicationOffset(12345678L),
        ),
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
            Set("party1", "party2"),
            Time.Timestamp.Epoch,
            Offset.firstOffset,
            "commandId",
            "transactionId",
            "applicationId",
            "synchronizer id",
            traceContext,
            submissionId,
            deduplicationOffset,
            deduplicationDurationSeconds,
            deduplicationDurationNanos,
          )

          val completion = completionStream.completionResponse.completion.value
          completion.synchronizerTime.value.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
          completion.offset shouldBe 1L

          completion.commandId shouldBe "commandId"
          completion.updateId shouldBe "transactionId"
          completion.applicationId shouldBe "applicationId"
          completion.submissionId shouldBe expectedSubmissionId
          completion.deduplicationPeriod shouldBe expectedDeduplicationPeriod
          completion.actAs.toSet shouldBe Set("party1", "party2")
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
            Set.empty,
            Time.Timestamp.Epoch,
            Offset.firstOffset,
            "commandId",
            "transactionId",
            "applicationId",
            "synchronizer id",
            traceContext,
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
        Set("party"),
        Time.Timestamp.Epoch,
        Offset.tryFromLong(2L),
        "commandId",
        status,
        "applicationId",
        "synchronizer id",
        traceContext,
        Some("submissionId"),
      )

      val completion = completionStream.completionResponse.completion.value
      completion.synchronizerTime.value.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
      completion.offset shouldBe 2L

      completion.commandId shouldBe "commandId"
      completion.applicationId shouldBe "applicationId"
      completion.submissionId shouldBe "submissionId"
      completion.status shouldBe Some(status)
      completion.actAs shouldBe Seq("party")
    }
  }
}

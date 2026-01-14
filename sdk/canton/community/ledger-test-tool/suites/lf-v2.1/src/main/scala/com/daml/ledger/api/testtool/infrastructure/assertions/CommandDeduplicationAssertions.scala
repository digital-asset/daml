// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.assertions

import com.daml.ledger.api.testtool.infrastructure.Assertions.assertDefined
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{Party, WithTimeout}
import com.daml.ledger.api.v2.completion.Completion
import com.google.protobuf.duration.Duration as DurationProto
import io.grpc.Status.Code

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.*

object CommandDeduplicationAssertions {

  def assertDeduplicationDuration(
      requestedDeduplicationDuration: DurationProto,
      completion: Completion,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val requestedDuration = DurationConversion.fromProto(requestedDeduplicationDuration)
    val reportedOffset = assertDefined(
      completion.deduplicationPeriod.deduplicationOffset,
      "No deduplication offset has been reported",
    )
    if (completion.getStatus.code == Code.ALREADY_EXISTS.value) {
      assertReportedOffsetForDuplicateSubmission(
        reportedOffset,
        completion,
        submittingParty,
        ledger,
      )
    } else {
      assertReportedOffsetForAcceptedSubmission(
        reportedOffset,
        requestedDuration,
        completion,
        submittingParty,
        ledger,
      )
    }
  }

  private def assertReportedOffsetForDuplicateSubmission(
      reportedOffset: Long,
      completion: Completion,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext) =
    WithTimeout(5.seconds)(
      ledger.findCompletionAtOffset(
        reportedOffset,
        c => c.commandId == completion.commandId && c.getStatus.code == Code.OK.value,
      )(submittingParty)
    ).map { optAcceptedCompletion =>
      val acceptedCompletion = assertDefined(
        optAcceptedCompletion,
        s"No accepted completion with the command ID '${completion.commandId}' since the reported offset $reportedOffset has been found",
      )
      assert(
        acceptedCompletion.offset < completion.offset,
        s"An accepted completion with the command ID '${completion.commandId}' at the offset ${acceptedCompletion.offset} that is not before the completion's offset ${completion.offset} has been found",
      )
    }

  private def assertReportedOffsetForAcceptedSubmission(
      reportedOffset: Long,
      requestedDuration: Duration,
      completion: Completion,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext) =
    WithTimeout(5.seconds)(
      ledger.findCompletionAtOffset(
        reportedOffset,
        _.commandId == completion.commandId,
      )(submittingParty)
    ).map { optReportedOffsetCompletion =>
      val reportedOffsetCompletion = assertDefined(
        optReportedOffsetCompletion,
        s"No completion with the command ID '${completion.commandId}' since the reported offset $reportedOffset has been found",
      )
      assert(
        reportedOffsetCompletion.offset == reportedOffset,
        s"No completion with the reported offset $reportedOffset has been found, the ${reportedOffsetCompletion.offset} offset has been found instead",
      )
      val durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes = Duration
        .between(
          reportedOffsetCompletion.getSynchronizerTime.getRecordTime.asJavaInstant,
          completion.getSynchronizerTime.getRecordTime.asJavaInstant,
        )
      assert(
        durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes >= requestedDuration,
        s"The requested deduplication duration $requestedDuration was greater than the duration between the reported deduplication offset and completion record times ($durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes).",
      )
    }

  def assertDeduplicationOffset(
      requestedDeduplicationOffsetCompletion: Completion,
      completion: Completion,
  ): Unit = {
    val reportedOffset = assertDefined(
      completion.deduplicationPeriod.deduplicationOffset,
      "No deduplication offset has been reported",
    )
    val requestedDeduplicationOffset =
      requestedDeduplicationOffsetCompletion.offset
    assert(
      reportedOffset <= requestedDeduplicationOffset,
      s"The reported deduplication offset $reportedOffset was more recent than the requested deduplication offset $requestedDeduplicationOffset.",
    )
  }

  object DurationConversion {

    def toProto(jDuration: Duration): DurationProto =
      DurationProto(jDuration.getSeconds, jDuration.getNano)

    def fromProto(pDuration: DurationProto): Duration =
      Duration.ofSeconds(pDuration.seconds, pDuration.nanos.toLong)
  }

}

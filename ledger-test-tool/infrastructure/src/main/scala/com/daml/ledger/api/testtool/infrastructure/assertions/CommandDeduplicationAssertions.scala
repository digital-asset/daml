// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.assertions

import java.time.Duration
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertDefined, fail}
import com.daml.ledger.api.testtool.infrastructure.WithTimeout
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.CompletionResponse
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.{
  DurationSupport,
  OffsetSupport,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.javaapi.data.Party
import com.daml.lf.data.Ref
import com.google.protobuf.duration.{Duration => DurationProto}
import io.grpc.Status.Code

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits._

object CommandDeduplicationAssertions {

  def assertDeduplicationDuration(
      requestedDeduplicationDuration: DurationProto,
      completionResponse: CompletionResponse,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val requestedDuration = DurationConversion.fromProto(requestedDeduplicationDuration)
    ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.durationSupport match {
      case DurationSupport.DURATION_NATIVE_SUPPORT =>
        Future {
          val reportedDurationProto = assertDefined(
            completionResponse.completion.deduplicationPeriod.deduplicationDuration,
            "No deduplication duration has been reported",
          )
          val reportedDuration = DurationConversion.fromProto(reportedDurationProto)
          assert(
            reportedDuration >= requestedDuration,
            s"The reported deduplication duration $reportedDuration was smaller than the requested deduplication duration $requestedDuration.",
          )
        }
      case DurationSupport.DURATION_CONVERT_TO_OFFSET =>
        val reportedOffset = assertDefined(
          completionResponse.completion.deduplicationPeriod.deduplicationOffset,
          "No deduplication offset has been reported",
        )
        val reportedHexOffset = Ref.HexString.assertFromString(reportedOffset)
        if (completionResponse.completion.getStatus.code == Code.ALREADY_EXISTS.value) {
          assertReportedOffsetForDuplicateSubmission(
            reportedHexOffset,
            completionResponse,
            submittingParty,
            ledger,
          )
        } else {
          assertReportedOffsetForAcceptedSubmission(
            reportedHexOffset,
            requestedDuration,
            completionResponse,
            submittingParty,
            ledger,
          )
        }
      case DurationSupport.Unrecognized(_) =>
        fail("Unrecognized deduplication duration support")
    }
  }

  private def assertReportedOffsetForDuplicateSubmission(
      reportedOffset: Ref.HexString,
      completionResponse: CompletionResponse,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext) =
    WithTimeout(5.seconds)(
      ledger.findCompletionAtOffset(
        reportedOffset,
        c => c.commandId == completionResponse.completion.commandId && c.getStatus.code == Code.OK.value,
      )(submittingParty)
    ).map { optAcceptedCompletionResponse =>
      val acceptedCompletionResponse = assertDefined(
        optAcceptedCompletionResponse,
        s"No accepted completion with the command ID '${completionResponse.completion.commandId}' since the reported offset $reportedOffset has been found",
      )
      assert(
        acceptedCompletionResponse.offset.getAbsolute < completionResponse.offset.getAbsolute,
        s"An accepted completion with the command ID '${completionResponse.completion.commandId}' at the offset ${acceptedCompletionResponse.offset} that is not before the completion's offset ${completionResponse.offset} has been found",
      )
    }

  private def assertReportedOffsetForAcceptedSubmission(
      reportedOffset: Ref.HexString,
      requestedDuration: Duration,
      completionResponse: CompletionResponse,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext) =
    WithTimeout(5.seconds)(
      ledger.findCompletionAtOffset(
        reportedOffset,
        _.commandId == completionResponse.completion.commandId,
      )(submittingParty)
    ).map { optReportedOffsetCompletionResponse =>
      val reportedOffsetCompletionResponse = assertDefined(
        optReportedOffsetCompletionResponse,
        s"No completion with the command ID '${completionResponse.completion.commandId}' since the reported offset $reportedOffset has been found",
      )
      assert(
        reportedOffsetCompletionResponse.offset == LedgerOffset.of(
          LedgerOffset.Value.Absolute(reportedOffset)
        ),
        s"No completion with the reported offset $reportedOffset has been found, the ${reportedOffsetCompletionResponse.offset} offset has been found instead",
      )
      val durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes = Duration
        .between(
          reportedOffsetCompletionResponse.recordTime,
          completionResponse.recordTime,
        )
      assert(
        durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes >= requestedDuration,
        s"The requested deduplication duration $requestedDuration was greater than the duration between the reported deduplication offset and completion record times ($durationBetweenReportedDeduplicationOffsetAndCompletionRecordTimes).",
      )
    }

  def assertDeduplicationOffset(
      requestedDeduplicationOffsetCompletionResponse: CompletionResponse,
      completionResponse: CompletionResponse,
      offsetSupport: OffsetSupport,
  ): Unit =
    offsetSupport match {
      case OffsetSupport.OFFSET_NATIVE_SUPPORT =>
        val reportedOffset = assertDefined(
          completionResponse.completion.deduplicationPeriod.deduplicationOffset,
          "No deduplication offset has been reported",
        )
        val requestedDeduplicationOffset =
          requestedDeduplicationOffsetCompletionResponse.offset.getAbsolute
        assert(
          reportedOffset <= requestedDeduplicationOffset,
          s"The reported deduplication offset $reportedOffset was more recent than the requested deduplication offset $requestedDeduplicationOffset.",
        )
      case OffsetSupport.OFFSET_CONVERT_TO_DURATION =>
        val reportedDurationProto = assertDefined(
          completionResponse.completion.deduplicationPeriod.deduplicationDuration,
          "No deduplication duration has been reported",
        )
        val reportedDuration = DurationConversion.fromProto(reportedDurationProto)
        val durationBetweenDeduplicationOffsetAndCompletionRecordTimes = Duration
          .between(
            requestedDeduplicationOffsetCompletionResponse.recordTime,
            completionResponse.recordTime,
          )
        assert(
          reportedDuration >= durationBetweenDeduplicationOffsetAndCompletionRecordTimes,
          s"The reported duration $reportedDuration was smaller than the duration between deduplication offset and completion record times ($durationBetweenDeduplicationOffsetAndCompletionRecordTimes).",
        )
      case OffsetSupport.Unrecognized(_) | OffsetSupport.OFFSET_NOT_SUPPORTED =>
        fail("Deduplication offsets are not supported")
    }

  object DurationConversion {

    def toProto(jDuration: Duration): DurationProto =
      DurationProto(jDuration.getSeconds, jDuration.getNano)

    def fromProto(pDuration: DurationProto): Duration =
      Duration.ofSeconds(pDuration.seconds, pDuration.nanos.toLong)
  }

}

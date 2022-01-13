// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.assertions

import java.time.{Duration, Instant}

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertDefined, fail}
import com.daml.ledger.api.testtool.infrastructure.participant.{
  CompletionResponse,
  ParticipantTestContext,
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.{
  DurationSupport,
  OffsetSupport,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding.Primitive.Party
import com.google.protobuf.duration.{Duration => DurationProto}
import io.grpc.Status.Code

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits._

object CommandDeduplicationAssertions {

  def assertDeduplicationDuration(
      requestedDeduplicationDuration: DurationProto,
      previousSubmissionSendTime: Instant,
      completionReceiveTime: Instant,
      completion: Completion,
      submittingParty: Party,
      ledger: ParticipantTestContext,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val requestedDuration = DurationConversion.fromProto(requestedDeduplicationDuration)
    ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.durationSupport match {
      case DurationSupport.DURATION_NATIVE_SUPPORT =>
        val reportedDurationProto = assertDefined(
          completion.deduplicationPeriod.deduplicationDuration,
          "No deduplication duration has been reported",
        )
        val reportedDuration = DurationConversion.fromProto(reportedDurationProto)
        assert(
          reportedDuration >= requestedDuration,
          s"The reported deduplication duration $reportedDuration was smaller than the requested deduplication duration $requestedDuration.",
        )
        Future.unit
      case DurationSupport.DURATION_CONVERT_TO_OFFSET =>
        val reportedOffset = assertDefined(
          completion.deduplicationPeriod.deduplicationOffset,
          "No deduplication offset has been reported",
        )
        if (completion.getStatus.code == Code.ALREADY_EXISTS.value) {
          val completionStreamRequest = ledger.completionStreamRequest(
            LedgerOffset.of(LedgerOffset.Value.Absolute(reportedOffset))
          )(submittingParty)
          ledger
            .findCompletion(completionStreamRequest)(_.commandId == completion.commandId)
            .map { optOffsetCompletionResponse =>
              val _ = assertDefined(
                optOffsetCompletionResponse,
                s"No completion with the command ID '${completion.commandId}' after the reported offset $reportedOffset has been found",
              )
            }
        } else {
          // Let t2 (completionReceiveTime) be the time when the application received the completion offset that specifies the deduplication offset off1.
          // Let t1 (previousSubmissionSendTime) be the time when the application sent off the submission that completed at off1.
          // Then t2 - t1 >= requested deduplication duration.
          assert(
            requestedDuration <= Duration
              .between(previousSubmissionSendTime, completionReceiveTime),
            s"The requested deduplication duration $requestedDeduplicationDuration was greater than the duration between sending the previous submission and receiving the next completion.",
          )
          Future.unit
        }
      case DurationSupport.Unrecognized(_) =>
        fail("Unrecognized deduplication duration support")
    }
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
}

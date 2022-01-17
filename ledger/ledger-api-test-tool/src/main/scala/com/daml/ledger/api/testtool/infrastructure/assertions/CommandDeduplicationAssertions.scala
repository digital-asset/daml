// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.assertions

import java.time.Duration

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertDefined, fail}
import com.daml.ledger.api.testtool.infrastructure.participant.{
  CompletionResponse,
  ParticipantTestContext,
}
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.{
  DurationSupport,
  OffsetSupport,
}
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.platform.testing.WithTimeout
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
        val reportedDurationProto = assertDefined(
          completionResponse.completion.deduplicationPeriod.deduplicationDuration,
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
          completionResponse.completion.deduplicationPeriod.deduplicationOffset,
          "No deduplication offset has been reported",
        )
        if (completionResponse.completion.getStatus.code == Code.ALREADY_EXISTS.value) {
          // Search for the first accepting completion with the same command ID since the beginning of the test case.
          // Multiple consecutive accepting completions are not supported.
          val completionStreamRequest = ledger.completionStreamRequest()(submittingParty)
          WithTimeout(5.seconds)(
            ledger
              .findCompletion(completionStreamRequest)(c =>
                c.commandId == c.commandId && c.getStatus.code == Code.OK.value
              )
          ).map { optAcceptedCompletionResponse =>
            val acceptedCompletionResponse = assertDefined(
              optAcceptedCompletionResponse,
              s"No accepted completion with the command ID '${completionResponse.completion.commandId}' has been found",
            )
            assert(
              acceptedCompletionResponse.offset.getAbsolute >= reportedOffset,
              s"No accepted completion with the command ID '${completionResponse.completion.commandId}' after the reported offset $reportedOffset has been found",
            )
            assert(
              acceptedCompletionResponse.offset.getAbsolute < completionResponse.offset.getAbsolute,
              s"No accepted completion with the command ID '${completionResponse.completion.commandId}' before the completion's offset ${completionResponse.offset} has been found",
            )
          }
        } else {
          fail("This case is not yet supported by this assertion")
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

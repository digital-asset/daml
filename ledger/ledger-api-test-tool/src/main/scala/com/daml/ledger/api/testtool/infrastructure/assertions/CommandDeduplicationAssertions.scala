// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.assertions

import java.time.Instant

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.testtool.infrastructure.participant.CompletionResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.{
  DurationSupport,
  OffsetSupport,
}
import com.daml.lf.data.Ref
import com.daml.scalautil.Statement.discard
import com.google.protobuf.duration.Duration

object CommandDeduplicationAssertions {
  def assertDeduplicationDuration(
      requestedDeduplicationDuration: Duration,
      previousSubmissionSendTime: Instant,
      completionReceiveTime: Instant,
      completion: Completion,
      durationSupport: DurationSupport,
  ): Unit = discard {
    val requestedDuration = DurationConversion.fromProto(requestedDeduplicationDuration)
    durationSupport match {
      case DurationSupport.DURATION_NATIVE_SUPPORT =>
        completion.deduplicationPeriod.deduplicationDuration.map { reportedDurationProto =>
          val reportedDuration = DurationConversion.fromProto(reportedDurationProto)
          assert(
            reportedDuration.compareTo(requestedDuration) >= 0,
            s"The reported deduplication duration $reportedDuration was smaller than the requested deduplication duration $requestedDuration.",
          )
        }
      case DurationSupport.DURATION_CONVERT_TO_OFFSET =>
        assert(
          requestedDuration
            .compareTo(
              java.time.Duration.between(previousSubmissionSendTime, completionReceiveTime)
            ) <= 0,
          s"The requested deduplication duration $requestedDeduplicationDuration was greater than the duration between sending the previous submission and receiving the next completion.",
        )
      case DurationSupport.Unrecognized(_) =>
        ()
    }
  }

  def assertDeduplicationOffset(
      requestedDeduplicationOffset: Ref.HexString,
      previousCompletionResponse: CompletionResponse,
      completionResponse: CompletionResponse,
      offsetSupport: OffsetSupport,
  ): Unit = discard {
    offsetSupport match {
      case OffsetSupport.OFFSET_NATIVE_SUPPORT =>
        completionResponse.completion.deduplicationPeriod.deduplicationOffset.map {
          reportedOffset =>
            assert(
              reportedOffset <= requestedDeduplicationOffset,
              s"The reported deduplication offset $reportedOffset was more recent than the requested deduplication offset $requestedDeduplicationOffset.",
            )
        }
      case OffsetSupport.OFFSET_CONVERT_TO_DURATION =>
        completionResponse.completion.deduplicationPeriod.deduplicationDuration.map {
          reportedDurationProto =>
            val reportedDuration = DurationConversion.fromProto(reportedDurationProto)
            val durationBetweenPreviousAndCurrentCompletionRecordTimes = java.time.Duration
              .between(previousCompletionResponse.recordTime, completionResponse.recordTime)
            assert(
              reportedDuration.compareTo(
                durationBetweenPreviousAndCurrentCompletionRecordTimes
              ) >= 0,
              s"The reported duration $reportedDuration was smaller than the duration between record times ($durationBetweenPreviousAndCurrentCompletionRecordTimes).",
            )
        }
      case OffsetSupport.Unrecognized(_) | OffsetSupport.OFFSET_NOT_SUPPORTED =>
        ()
    }
  }
}

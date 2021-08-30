// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.ledger.api.v1.command_completion_service.{Checkpoint, CompletionStreamResponse}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.offset.Offset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.google.protobuf.duration.Duration
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given application and set of parties
// TODO Restrict the scope of this to com.daml.platform.store.dao when
// TODO - the in-memory sandbox is gone
private[platform] object CompletionFromTransaction {
  private val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionTransactionId = ""

  def acceptedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      transactionId: String,
      applicationId: String,
      maybeSubmissionId: Option[String] = None,
      maybeDeduplicationOffset: Option[String] = None,
      maybeDeduplicationTimeSeconds: Option[Long] = None,
      maybeDeduplicationTimeNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(
        toApiCompletion(
          commandId = commandId,
          transactionId = transactionId,
          applicationId = applicationId,
          maybeStatus = Some(OkStatus),
          maybeSubmissionId = maybeSubmissionId,
          maybeDeduplicationOffset = maybeDeduplicationOffset,
          maybeDeduplicationTimeSeconds = maybeDeduplicationTimeSeconds,
          maybeDeduplicationTimeNanos = maybeDeduplicationTimeNanos,
        )
      ),
    )

  def rejectedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      status: StatusProto,
      applicationId: String,
      maybeSubmissionId: Option[String] = None,
      maybeDeduplicationOffset: Option[String] = None,
      maybeDeduplicationTimeSeconds: Option[Long] = None,
      maybeDeduplicationTimeNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(
        toApiCompletion(
          commandId = commandId,
          transactionId = RejectionTransactionId,
          applicationId = applicationId,
          maybeStatus = Some(status),
          maybeSubmissionId = maybeSubmissionId,
          maybeDeduplicationOffset = maybeDeduplicationOffset,
          maybeDeduplicationTimeSeconds = maybeDeduplicationTimeSeconds,
          maybeDeduplicationTimeNanos = maybeDeduplicationTimeNanos,
        )
      ),
    )

  private def toApiCheckpoint(recordTime: Instant, offset: Offset): Checkpoint =
    Checkpoint.of(
      recordTime = Some(fromInstant(recordTime)),
      offset = Some(LedgerOffset.of(LedgerOffset.Value.Absolute(offset.toApiString))),
    )

  private def toApiCompletion(
      commandId: String,
      transactionId: String,
      applicationId: String,
      maybeStatus: Option[StatusProto],
      maybeSubmissionId: Option[String],
      maybeDeduplicationOffset: Option[String],
      maybeDeduplicationTimeSeconds: Option[Long],
      maybeDeduplicationTimeNanos: Option[Int],
  ): Completion = {
    val maybeDeduplicationPeriod = toApiDeduplicationPeriod(
      maybeDeduplicationOffset = maybeDeduplicationOffset,
      maybeDeduplicationTimeSeconds = maybeDeduplicationTimeSeconds,
      maybeDeduplicationTimeNanos = maybeDeduplicationTimeNanos,
    )
    (maybeSubmissionId, maybeDeduplicationPeriod) match {
      case (Some(submissionId), Some(deduplicationPeriod)) =>
        Completion(
          commandId = commandId,
          status = maybeStatus,
          transactionId = transactionId,
          applicationId = applicationId,
          submissionId = submissionId,
          deduplicationPeriod = deduplicationPeriod,
        )
      case (Some(submissionId), _) =>
        Completion(
          commandId = commandId,
          status = maybeStatus,
          transactionId = transactionId,
          applicationId = applicationId,
          submissionId = submissionId,
        )
      case (None, Some(deduplicationPeriod)) =>
        Completion(
          commandId = commandId,
          status = maybeStatus,
          transactionId = transactionId,
          applicationId = applicationId,
          deduplicationPeriod = deduplicationPeriod,
        )
      case _ =>
        Completion(
          commandId = commandId,
          status = Some(OkStatus),
          transactionId = transactionId,
          applicationId = applicationId,
        )
    }
  }

  private def toApiDeduplicationPeriod(
      maybeDeduplicationOffset: Option[String],
      maybeDeduplicationTimeSeconds: Option[Long],
      maybeDeduplicationTimeNanos: Option[Int],
  ): Option[Completion.DeduplicationPeriod] =
    // The only invariant that should hold, considering legacy data, is that either
    // the deduplication time seconds and nanos are both populated, or neither is.
    (maybeDeduplicationOffset, (maybeDeduplicationTimeSeconds, maybeDeduplicationTimeNanos)) match {
      case (None, (None, None)) => None
      case (Some(offset), _) =>
        Some(Completion.DeduplicationPeriod.DeduplicationOffset(offset))
      case (_, (Some(deduplicationTimeSeconds), Some(deduplicationTimeNanos))) =>
        Some(
          Completion.DeduplicationPeriod.DeduplicationTime(
            new Duration(
              seconds = deduplicationTimeSeconds,
              nanos = deduplicationTimeNanos,
            )
          )
        )
      case _ =>
        throw new IllegalArgumentException(
          "One of deduplication time seconds and nanos has been provided " +
            "but they must be either both provided or both absent"
        )
    }
}

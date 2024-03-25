// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.ApiOffset.ApiOffsetConverter
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.google.protobuf.duration.Duration
import com.google.rpc.status.Status as StatusProto
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given application and set of parties
private[platform] object CompletionFromTransaction {
  private val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionTransactionId = ""

  def acceptedCompletion(
      recordTime: Timestamp,
      offset: Offset,
      commandId: String,
      transactionId: String,
      applicationId: String,
      domainId: Option[String],
      traceContext: TraceContext,
      optSubmissionId: Option[String] = None,
      optDeduplicationOffset: Option[String] = None,
      optDeduplicationDurationSeconds: Option[Long] = None,
      optDeduplicationDurationNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completion = Some(
        toApiCompletion(
          commandId = commandId,
          transactionId = transactionId,
          applicationId = applicationId,
          traceContext = traceContext,
          optStatus = Some(OkStatus),
          optSubmissionId = optSubmissionId,
          optDeduplicationOffset = optDeduplicationOffset,
          optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
          optDeduplicationDurationNanos = optDeduplicationDurationNanos,
        )
      ),
      domainId = domainId.getOrElse(""),
    )

  def rejectedCompletion(
      recordTime: Timestamp,
      offset: Offset,
      commandId: String,
      status: StatusProto,
      applicationId: String,
      domainId: Option[String],
      traceContext: TraceContext,
      optSubmissionId: Option[String] = None,
      optDeduplicationOffset: Option[String] = None,
      optDeduplicationDurationSeconds: Option[Long] = None,
      optDeduplicationDurationNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completion = Some(
        toApiCompletion(
          commandId = commandId,
          transactionId = RejectionTransactionId,
          applicationId = applicationId,
          traceContext = traceContext,
          optStatus = Some(status),
          optSubmissionId = optSubmissionId,
          optDeduplicationOffset = optDeduplicationOffset,
          optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
          optDeduplicationDurationNanos = optDeduplicationDurationNanos,
        )
      ),
      domainId = domainId.getOrElse(""),
    )

  private def toApiCheckpoint(recordTime: Timestamp, offset: Offset): Checkpoint =
    Checkpoint.of(
      recordTime = Some(fromInstant(recordTime.toInstant)),
      offset = Some(LedgerOffset.of(LedgerOffset.Value.Absolute(offset.toApiString))),
    )

  private[store] def toApiCompletion(
      commandId: String,
      transactionId: String,
      applicationId: String,
      traceContext: TraceContext,
      optStatus: Option[StatusProto],
      optSubmissionId: Option[String],
      optDeduplicationOffset: Option[String],
      optDeduplicationDurationSeconds: Option[Long],
      optDeduplicationDurationNanos: Option[Int],
  ): Completion = {
    val completionWithMandatoryFields = Completion(
      commandId = commandId,
      status = optStatus,
      updateId = transactionId,
      applicationId = applicationId,
      traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
    )
    val optDeduplicationPeriod = toApiDeduplicationPeriod(
      optDeduplicationOffset = optDeduplicationOffset,
      optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
      optDeduplicationDurationNanos = optDeduplicationDurationNanos,
    )
    (optSubmissionId, optDeduplicationPeriod) match {
      case (Some(submissionId), Some(deduplicationPeriod)) =>
        completionWithMandatoryFields.copy(
          submissionId = submissionId,
          deduplicationPeriod = deduplicationPeriod,
        )
      case (Some(submissionId), None) =>
        completionWithMandatoryFields.copy(
          submissionId = submissionId
        )
      case (None, Some(deduplicationPeriod)) =>
        completionWithMandatoryFields.copy(
          deduplicationPeriod = deduplicationPeriod
        )
      case _ =>
        completionWithMandatoryFields
    }
  }

  private def toApiDeduplicationPeriod(
      optDeduplicationOffset: Option[String],
      optDeduplicationDurationSeconds: Option[Long],
      optDeduplicationDurationNanos: Option[Int],
  ): Option[Completion.DeduplicationPeriod] =
    // The only invariant that should hold, considering legacy data, is that either
    // the deduplication duration seconds and nanos are both populated, or neither is.
    (
      optDeduplicationOffset,
      (optDeduplicationDurationSeconds, optDeduplicationDurationNanos),
    ) match {
      case (None, (None, None)) => None
      case (Some(offset), _) =>
        Some(Completion.DeduplicationPeriod.DeduplicationOffset(offset))
      case (_, (Some(deduplicationDurationSeconds), Some(deduplicationDurationNanos))) =>
        Some(
          Completion.DeduplicationPeriod.DeduplicationDuration(
            new Duration(
              seconds = deduplicationDurationSeconds,
              nanos = deduplicationDurationNanos,
            )
          )
        )
      case _ =>
        throw new IllegalArgumentException(
          "One of deduplication duration seconds and nanos has been provided " +
            "but they must be either both provided or both absent"
        )
    }
}

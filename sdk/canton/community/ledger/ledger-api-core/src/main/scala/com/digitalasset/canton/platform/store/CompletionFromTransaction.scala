// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse.CompletionResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.completion.Completion.DeduplicationPeriod.Empty
import com.daml.ledger.api.v2.offset_checkpoint.SynchronizerTime
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.duration.Duration
import com.google.rpc.status.Status as StatusProto
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given user and set of parties
object CompletionFromTransaction {
  val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionUpdateId = ""

  /** Properties defined for both accepted and rejected commands */
  final case class CommonCompletionProperties(
      submitters: Set[String],
      completionOffset: Long,
      synchronizerTime: Option[SynchronizerTime],
      commandId: String,
      userId: String,
      submissionId: Option[String],
      traceContext: TraceContext,
      deduplicationOffset: Option[Long],
      deduplicationDurationSeconds: Option[Long],
      deduplicationDurationNanos: Option[Int],
  ) {}

  object CommonCompletionProperties {
    def createFromRecordTimeAndSynchronizerId(
        submitters: Set[String],
        recordTime: Timestamp,
        completionOffset: Offset,
        commandId: String,
        userId: String,
        submissionId: Option[String],
        synchronizerId: String,
        traceContext: TraceContext,
        deduplicationOffset: Option[Long],
        deduplicationDurationSeconds: Option[Long],
        deduplicationDurationNanos: Option[Int],
    ): CommonCompletionProperties = CommonCompletionProperties(
      submitters = submitters,
      completionOffset = completionOffset.unwrap,
      synchronizerTime = Some(toApiSynchronizerTime(synchronizerId, recordTime)),
      commandId = commandId,
      userId = userId,
      submissionId = submissionId,
      traceContext = traceContext,
      deduplicationOffset = deduplicationOffset,
      deduplicationDurationSeconds = deduplicationDurationSeconds,
      deduplicationDurationNanos = deduplicationDurationNanos,
    )
  }

  def acceptedCompletion(
      commonCompletionProperties: CommonCompletionProperties,
      updateId: UpdateId,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      completionResponse = CompletionResponse.Completion(
        toApiCompletion(
          commonCompletionProperties,
          updateId = updateId.toHexString,
          optStatus = Some(OkStatus),
        )
      )
    )

  def rejectedCompletion(
      commonCompletionProperties: CommonCompletionProperties,
      status: StatusProto,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      completionResponse = CompletionResponse.Completion(
        toApiCompletion(
          commonCompletionProperties = commonCompletionProperties,
          updateId = RejectionUpdateId,
          optStatus = Some(status),
        )
      )
    )

  private def toApiSynchronizerTime(
      synchronizerId: String,
      recordTime: Timestamp,
  ): SynchronizerTime =
    SynchronizerTime.of(
      synchronizerId = synchronizerId,
      recordTime = Some(fromInstant(recordTime.toInstant)),
    )

  def toApiCompletion(
      commonCompletionProperties: CommonCompletionProperties,
      updateId: String,
      optStatus: Option[StatusProto],
  ): Completion = {
    val optDeduplicationPeriod = toApiDeduplicationPeriod(
      optDeduplicationOffset = commonCompletionProperties.deduplicationOffset,
      optDeduplicationDurationSeconds = commonCompletionProperties.deduplicationDurationSeconds,
      optDeduplicationDurationNanos = commonCompletionProperties.deduplicationDurationNanos,
    )

    Completion(
      commandId = commonCompletionProperties.commandId,
      status = optStatus,
      updateId = updateId,
      userId = commonCompletionProperties.userId,
      actAs = commonCompletionProperties.submitters.toSeq,
      submissionId = commonCompletionProperties.submissionId.getOrElse(""),
      deduplicationPeriod = optDeduplicationPeriod.getOrElse(Empty),
      traceContext =
        SerializableTraceContext(commonCompletionProperties.traceContext).toDamlProtoOpt,
      offset = commonCompletionProperties.completionOffset,
      synchronizerTime = commonCompletionProperties.synchronizerTime,
    )
  }

  private def toApiDeduplicationPeriod(
      optDeduplicationOffset: Option[Long],
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
        Some(
          Completion.DeduplicationPeriod.DeduplicationOffset(offset)
        )
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

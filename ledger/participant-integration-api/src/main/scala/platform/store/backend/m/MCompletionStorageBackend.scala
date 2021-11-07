// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.{ApplicationId, Party}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.backend.common.CompletionStorageBackendTemplate
import com.daml.platform.store.backend.CompletionStorageBackend

object MCompletionStorageBackend extends CompletionStorageBackend {
  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(connection: Connection): List[CompletionStreamResponse] = {
    import com.daml.platform.store.backend.m.SearchUtil.rangeIterator
    val completions = MStore(connection).mData.commandCompletions
    val startExclusiveOffset = startExclusive.toHexString.toString match {
      case "" => None
      case nonempty => Some(nonempty)
    }
    val endInclusiveOffset = endInclusive.toHexString.toString
    val stringParties = parties.map(_.toString)
    rangeIterator(startExclusiveOffset, endInclusiveOffset, completions)(_.completion_offset)
      .filter(_.application_id == applicationId.toString)
      .filter(_.submitters.exists(stringParties))
      .map(dto =>
        dto.transaction_id match {
          case Some(txId) =>
            CompletionFromTransaction.acceptedCompletion(
              recordTime = Time.Timestamp(dto.record_time),
              offset = Offset.fromHexString(Ref.HexString.assertFromString(dto.completion_offset)),
              commandId = dto.command_id,
              transactionId = txId,
              applicationId = dto.application_id,
              optSubmissionId = dto.submission_id,
              optDeduplicationOffset = dto.deduplication_offset,
              optDeduplicationDurationSeconds = dto.deduplication_duration_seconds,
              optDeduplicationDurationNanos = dto.deduplication_duration_nanos,
            )

          case None =>
            CompletionFromTransaction.rejectedCompletion(
              recordTime = Time.Timestamp(dto.record_time),
              offset = Offset.fromHexString(Ref.HexString.assertFromString(dto.completion_offset)),
              commandId = dto.command_id,
              status = CompletionStorageBackendTemplate.buildStatusProto(
                rejectionStatusCode = dto.rejection_status_code.get,
                rejectionStatusMessage = dto.rejection_status_message.get,
                rejectionStatusDetails = dto.rejection_status_details,
              ),
              applicationId = dto.application_id,
              optSubmissionId = dto.submission_id,
              optDeduplicationOffset = dto.deduplication_offset,
              optDeduplicationDurationSeconds = dto.deduplication_duration_seconds,
              optDeduplicationDurationNanos = dto.deduplication_duration_nanos,
            )
        }
      )
      .toList
  }

  override def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    val mStore = MStore(connection)
    val stringPruntUpToInclusive: String = pruneUpToInclusive.toHexString
    mStore.synchronized {
      mStore.mData = mStore.mData.copy(
        commandCompletions = mStore.mData.commandCompletions
          .dropWhile(_.completion_offset <= stringPruntUpToInclusive)
      )
    }
  }
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{byteArray, int, long, str}
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.Conversions.{instantFromMicros, offset}
import com.daml.platform.store.backend.CompletionStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.cache.StringInterningCache
import com.google.protobuf.any
import com.google.rpc.status.{Status => StatusProto}

trait CompletionStorageBackendTemplate extends CompletionStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  def queryStrategy: QueryStrategy

  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Party],
      stringInterningCache: StringInterningCache,
  )(connection: Connection): List[CompletionStreamResponse] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    import ComposableQuery._
    SQL"""
        SELECT
          completion_offset,
          record_time,
          command_id,
          transaction_id,
          rejection_status_code,
          rejection_status_message,
          rejection_status_details,
          application_id,
          submission_id,
          deduplication_offset,
          deduplication_duration_seconds,
          deduplication_duration_nanos,
          deduplication_start
        FROM
          participant_command_completions
        WHERE
          ($startExclusive is null or completion_offset > $startExclusive) AND
          completion_offset <= $endInclusive AND
          application_id = $applicationId AND
          ${queryStrategy.arrayIntersectionNonEmptyClause(
      "submitters",
      parties,
      stringInterningCache,
    )}
        ORDER BY completion_offset ASC"""
      .as(completionParser.*)(connection)
  }

  private val sharedColumns: RowParser[Offset ~ Instant ~ String ~ String ~ Option[String]] =
    offset("completion_offset") ~
      instantFromMicros("record_time") ~
      str("command_id") ~
      str("application_id") ~
      str("submission_id").?

  private val acceptedCommandSharedColumns
      : RowParser[Offset ~ Instant ~ String ~ String ~ Option[String] ~ String] =
    sharedColumns ~ str("transaction_id")

  private val deduplicationOffsetColumn: RowParser[Option[String]] =
    str("deduplication_offset").?
  private val deduplicationDurationSecondsColumn: RowParser[Option[Long]] =
    long("deduplication_duration_seconds").?
  private val deduplicationDurationNanosColumn: RowParser[Option[Int]] =
    int("deduplication_duration_nanos").?
  private val deduplicationStartColumn: RowParser[Option[Instant]] =
    instantFromMicros("deduplication_start").?

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    acceptedCommandSharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationDurationSecondsColumn ~ deduplicationDurationNanosColumn ~
      deduplicationStartColumn map {
        case offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~ transactionId ~
            deduplicationOffset ~ deduplicationDurationSeconds ~ deduplicationDurationNanos ~ _ =>
          CompletionFromTransaction.acceptedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            transactionId = transactionId,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
          )
      }

  private val rejectionStatusCodeColumn: RowParser[Int] = int("rejection_status_code")
  private val rejectionStatusMessageColumn: RowParser[String] = str("rejection_status_message")
  private val rejectionStatusDetailsColumn: RowParser[Option[Array[Byte]]] =
    byteArray("rejection_status_details").?

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationDurationSecondsColumn ~ deduplicationDurationNanosColumn ~
      deduplicationStartColumn ~
      rejectionStatusCodeColumn ~
      rejectionStatusMessageColumn ~
      rejectionStatusDetailsColumn map {
        case offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~
            deduplicationOffset ~ deduplicationDurationSeconds ~ deduplicationDurationNanos ~ _ ~
            rejectionStatusCode ~ rejectionStatusMessage ~ rejectionStatusDetails =>
          val status =
            buildStatusProto(rejectionStatusCode, rejectionStatusMessage, rejectionStatusDetails)
          CompletionFromTransaction.rejectedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            status = status,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
          )
      }

  private val completionParser: RowParser[CompletionStreamResponse] =
    acceptedCommandParser | rejectedCommandParser

  private def buildStatusProto(
      rejectionStatusCode: Int,
      rejectionStatusMessage: String,
      rejectionStatusDetails: Option[Array[Byte]],
  ): StatusProto =
    StatusProto.of(
      rejectionStatusCode,
      rejectionStatusMessage,
      parseRejectionStatusDetails(rejectionStatusDetails),
    )

  private def parseRejectionStatusDetails(
      rejectionStatusDetails: Option[Array[Byte]]
  ): Seq[any.Any] =
    rejectionStatusDetails
      .map(StatusDetails.parseFrom)
      .map(_.details)
      .getOrElse(Seq.empty)

  def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    pruneWithLogging(queryDescription = "Command completions pruning") {
      import com.daml.platform.store.Conversions.OffsetToStatement
      SQL"delete from participant_command_completions where completion_offset <= $pruneUpToInclusive"
    }(connection, loggingContext)
  }

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(loggingContext)
  }
}

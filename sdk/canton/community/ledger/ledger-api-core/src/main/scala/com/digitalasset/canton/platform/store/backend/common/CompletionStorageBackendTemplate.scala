// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.backend.CompletionStorageBackend
import com.digitalasset.canton.platform.store.backend.Conversions.{
  offset,
  timestampFromMicros,
  traceContextOption,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ApplicationId, Party}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.any
import com.google.rpc.status.Status as StatusProto

import java.sql.Connection

class CompletionStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
    val loggerFactory: NamedLoggerFactory,
) extends CompletionStorageBackend
    with NamedLogging {
  import com.digitalasset.canton.platform.store.backend.Conversions.ArrayColumnToIntArray.*

  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse] = {
    import ComposableQuery.*
    import com.digitalasset.canton.platform.store.backend.Conversions.applicationIdToStatement
    import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
    val internedParties =
      parties.view.map(stringInterning.party.tryInternalize).flatMap(_.toList).toSet
    if (internedParties.isEmpty) {
      Vector.empty
    } else {
      val rows = SQL"""
        SELECT
          submitters,
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
          deduplication_start,
          domain_id,
          trace_context
        FROM
          lapi_command_completions
        WHERE
          ${queryStrategy.offsetIsBetween(
          nonNullableColumn = "completion_offset",
          startExclusive = startExclusive,
          endInclusive = endInclusive,
        )} AND
          application_id = $applicationId
        ORDER BY completion_offset ASC
        ${QueryStrategy.limitClause(Some(limit))}"""
        .asVectorOf(completionParser)(connection)
      rows.collect {
        case (submitters, response) if submitters.exists(internedParties) => response
      }
    }
  }

  private val sharedColumns: RowParser[
    Array[Int] ~ Offset ~ Timestamp ~ String ~ String ~ Option[String] ~ Int ~ TraceContext
  ] = {
    array[Int]("submitters") ~
      offset("completion_offset") ~
      timestampFromMicros("record_time") ~
      str("command_id") ~
      str("application_id") ~
      str("submission_id").? ~
      int("domain_id") ~
      traceContextOption("trace_context")(noTracingLogger)
  }

  private val acceptedCommandSharedColumns: RowParser[
    Array[Int] ~ Offset ~ Timestamp ~ String ~ String ~ Option[String] ~ Int ~ TraceContext ~ String
  ] =
    sharedColumns ~ str("transaction_id")

  private val deduplicationOffsetColumn: RowParser[Option[String]] =
    str("deduplication_offset").?
  private val deduplicationDurationSecondsColumn: RowParser[Option[Long]] =
    long("deduplication_duration_seconds").?
  private val deduplicationDurationNanosColumn: RowParser[Option[Int]] =
    int("deduplication_duration_nanos").?
  private val deduplicationStartColumn: RowParser[Option[Timestamp]] =
    timestampFromMicros("deduplication_start").?

  private val acceptedCommandParser: RowParser[(Array[Int], CompletionStreamResponse)] =
    acceptedCommandSharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationDurationSecondsColumn ~ deduplicationDurationNanosColumn ~
      deduplicationStartColumn map {
        case submitters ~ offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~ internedDomainId ~ traceContext ~ transactionId ~
            deduplicationOffset ~ deduplicationDurationSeconds ~ deduplicationDurationNanos ~ _ =>
          submitters -> CompletionFromTransaction.acceptedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            transactionId = transactionId,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
            domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
            traceContext = traceContext,
          )
      }

  private val rejectionStatusCodeColumn: RowParser[Int] = int("rejection_status_code")
  private val rejectionStatusMessageColumn: RowParser[String] = str("rejection_status_message")
  private val rejectionStatusDetailsColumn: RowParser[Option[Array[Byte]]] =
    byteArray("rejection_status_details").?

  private val rejectedCommandParser: RowParser[(Array[Int], CompletionStreamResponse)] =
    sharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationDurationSecondsColumn ~ deduplicationDurationNanosColumn ~
      deduplicationStartColumn ~
      rejectionStatusCodeColumn ~
      rejectionStatusMessageColumn ~
      rejectionStatusDetailsColumn map {
        case submitters ~ offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~ internedDomainId ~ traceContext ~
            deduplicationOffset ~ deduplicationDurationSeconds ~ deduplicationDurationNanos ~ _ ~
            rejectionStatusCode ~ rejectionStatusMessage ~ rejectionStatusDetails =>
          val status =
            buildStatusProto(rejectionStatusCode, rejectionStatusMessage, rejectionStatusDetails)
          submitters -> CompletionFromTransaction.rejectedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            status = status,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
            domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
            traceContext = traceContext,
          )
      }

  private val completionParser: RowParser[(Array[Int], CompletionStreamResponse)] =
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

  override def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, traceContext: TraceContext): Unit = {
    pruneWithLogging(queryDescription = "Command completions pruning") {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      SQL"delete from lapi_command_completions where completion_offset <= $pruneUpToInclusive"
    }(connection, traceContext)
  }

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(
      traceContext
    )
  }
}

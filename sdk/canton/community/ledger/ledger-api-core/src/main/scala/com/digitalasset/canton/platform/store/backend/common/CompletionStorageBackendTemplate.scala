// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, SimpleSql}
import cats.syntax.all.*
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.backend.RowDef.column
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  Conversions,
  RowDef,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Party, SubmissionId, UserId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.any
import com.google.rpc.status.Status as StatusProto

import java.sql.Connection
import java.util.UUID

class CompletionStorageBackendTemplate(
    stringInterning: StringInterning,
    val loggerFactory: NamedLoggerFactory,
) extends CompletionStorageBackend
    with NamedLogging {

  object RowDefs {
    import CommonRowDefs.*

    def userId(stringInterning: StringInterning): RowDef[UserId] =
      column("user_id", int).map(stringInterning.userId.externalize)
    val messageUuid: RowDef[UUID] = column("message_uuid", str).map(UUID.fromString)
    val submissionId: RowDef[SubmissionId] =
      column("submission_id", str).map(SubmissionId.assertFromString)
    val completionOffset: RowDef[Offset] = column("completion_offset", Conversions.offset)

    // rejection status
    private val rejectionStatusCode: RowDef[Int] = column("rejection_status_code", int)
    private val rejectionStatusMessage: RowDef[String] = column("rejection_status_message", str)
    private val rejectionStatusDetails: RowDef[Option[Array[Byte]]] =
      column("rejection_status_details", byteArray).?
    private val rejectionStatus: RowDef[StatusProto] =
      (rejectionStatusCode, rejectionStatusMessage, rejectionStatusDetails).mapN(buildStatusProto)

    // deduplication offset
    val deduplicationOffset: RowDef[Option[Long]] = column("deduplication_offset", long).?
    val deduplicationDurationSeconds: RowDef[Option[Long]] =
      column("deduplication_duration_seconds", long).?
    val deduplicationDurationNanos: RowDef[Option[Int]] =
      column("deduplication_duration_nanos", int).?

    // post publish related
    private val isTransaction: RowDef[Boolean] = column("is_transaction", bool)

    private val publishSource: RowDef[PublishSource] =
      (messageUuid.?, recordTime).mapN(publishSourceFromColumns)

    private def commandCompletionSharedColumns(
        stringInterning: StringInterning,
        parties: Set[Party],
    ): RowDef[CompletionFromTransaction.CommonCompletionProperties] = (
      submitters(stringInterning).map(_.view.filter(parties).toSet[String]),
      recordTime,
      completionOffset,
      commandId,
      userId(stringInterning),
      submissionId.?,
      synchronizerId(stringInterning).map(_.toProtoPrimitive),
      traceContext.?.map(Conversions.traceContextOption(_)(noTracingLogger)),
      deduplicationOffset,
      deduplicationDurationSeconds,
      deduplicationDurationNanos,
    ).mapN(
      CompletionFromTransaction.CommonCompletionProperties.createFromRecordTimeAndSynchronizerId
    )

    def commandCompletionParser(
        parties: Set[Party]
    ): RowDef[CompletionStreamResponse] = updateId.?.map(_.isDefined).branch(
      (true, acceptedCommand(parties)),
      (false, rejectedCommand(parties)),
    )

    private def acceptedCommand(
        parties: Set[Party]
    ): RowDef[CompletionStreamResponse] = (
      commandCompletionSharedColumns(stringInterning, parties),
      updateId,
    ).mapN(CompletionFromTransaction.acceptedCompletion)

    private def rejectedCommand(
        parties: Set[Party]
    ): RowDef[CompletionStreamResponse] = (
      commandCompletionSharedColumns(stringInterning, parties),
      rejectionStatus,
    ).mapN(CompletionFromTransaction.rejectedCompletion)

    private def postPublishDataForTransactionParser: RowDef[PostPublishData] = (
      synchronizerId(stringInterning),
      publishSource,
      userId(stringInterning),
      commandId,
      submitters(stringInterning).map(_.toSet),
      completionOffset,
      publicationTime.map(CantonTimestamp.apply),
      submissionId.?,
      updateId.?.map(_.isDefined),
      traceContext.?.map(Conversions.traceContextOption(_)(noTracingLogger)),
    ).mapN(PostPublishData.apply)

    def postPublishDataParser: RowDef[Option[PostPublishData]] = isTransaction.branch(
      (true, postPublishDataForTransactionParser.map(Some(_))),
      (false, RowDef.static(None)),
    )

    private def publishSourceFromColumns(messageUuid: Option[UUID], recordTime: Timestamp) =
      messageUuid
        .map(PublishSource.Local(_): PublishSource)
        .getOrElse(
          PublishSource.Sequencer(
            CantonTimestamp(recordTime)
          )
        )
  }

  override def commandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      userId: UserId,
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse] = {
    import ComposableQuery.*
    if (parties.isEmpty) {
      Vector.empty
    } else {
      stringInterning.userId.tryInternalize(userId) match {
        case Some(internedUserId) =>
          val query = (columns: CompositeSql) =>
            SQL"""
            SELECT
              $columns
            FROM
              lapi_command_completions
            WHERE
              ${QueryStrategy.offsetIsBetween(
                nonNullableColumn = "completion_offset",
                startInclusive = startInclusive,
                endInclusive = endInclusive,
              )} AND
              user_id = $internedUserId
            ORDER BY completion_offset ASC
            ${QueryStrategy.limitClause(Some(limit))}"""

          RowDefs.commandCompletionParser(parties).queryMultipleRows(query)(connection).collect {
            case response if response.getCompletion.actAs.nonEmpty =>
              response
          }
        case None => Vector.empty
      }
    }
  }

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
  )(connection: Connection, traceContext: TraceContext): Unit =
    pruneWithLogging(queryDescription = "Command completions pruning") {
      import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
      SQL"delete from lapi_command_completions where completion_offset <= $pruneUpToInclusive"
    }(connection, traceContext)

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(
      traceContext
    )
  }

  override def commandCompletionsForRecovery(
      startInclusive: Offset,
      endInclusive: Offset,
  )(connection: Connection): Vector[PostPublishData] = {
    import ComposableQuery.*
    def query(columns: CompositeSql) = SQL"""
      SELECT
        $columns
      FROM
        lapi_command_completions
      WHERE
        ${QueryStrategy.offsetIsBetween(
        nonNullableColumn = "completion_offset",
        startInclusive = startInclusive,
        endInclusive = endInclusive,
      )}
      ORDER BY completion_offset ASC"""

    RowDefs.postPublishDataParser.queryMultipleRows(query)(connection).flatten
  }
}

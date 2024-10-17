// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.backend.CompletionStorageBackend
import com.digitalasset.canton.platform.store.backend.Conversions.{
  offset,
  timestampFromMicros,
  traceContextOption,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ApiOffset, ApplicationId, Party}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
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

  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse] = {
    import ComposableQuery.*
    import com.digitalasset.canton.platform.store.backend.Conversions.applicationIdToStatement
    import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
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
          update_id,
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
          ${QueryStrategy.offsetIsBetween(
          nonNullableColumn = "completion_offset",
          startExclusive = startExclusive,
          endInclusive = endInclusive,
        )} AND
          application_id = $applicationId
        ORDER BY completion_offset ASC
        ${QueryStrategy.limitClause(Some(limit))}"""
        .asVectorOf(completionParser(internedParties))(connection)
      rows.collect {
        case (submitters, response) if submitters.exists(internedParties) => response
      }
    }
  }

  private val sharedColumns: RowParser[
    Array[Int] ~ Offset ~ Timestamp ~ String ~ String ~ Option[String] ~ Int ~ TraceContext
  ] =
    array[Int]("submitters") ~
      offset("completion_offset") ~
      timestampFromMicros("record_time") ~
      str("command_id") ~
      str("application_id") ~
      str("submission_id").? ~
      int("domain_id") ~
      traceContextOption("trace_context")(noTracingLogger)

  private val acceptedCommandSharedColumns: RowParser[
    Array[Int] ~ Offset ~ Timestamp ~ String ~ String ~ Option[String] ~ Int ~ TraceContext ~ String
  ] =
    sharedColumns ~ str("update_id")

  private val deduplicationOffsetColumn: RowParser[Option[String]] =
    str("deduplication_offset").?
  private val deduplicationDurationSecondsColumn: RowParser[Option[Long]] =
    long("deduplication_duration_seconds").?
  private val deduplicationDurationNanosColumn: RowParser[Option[Int]] =
    int("deduplication_duration_nanos").?
  private val deduplicationStartColumn: RowParser[Option[Timestamp]] =
    timestampFromMicros("deduplication_start").?

  private def acceptedCommandParser(
      internedParties: Set[Int]
  ): RowParser[(Array[Int], CompletionStreamResponse)] =
    acceptedCommandSharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationDurationSecondsColumn ~ deduplicationDurationNanosColumn ~
      deduplicationStartColumn map {
        case submitters ~ offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~ internedDomainId ~ traceContext ~ updateId ~
            deduplicationOffset ~ deduplicationDurationSeconds ~ deduplicationDurationNanos ~ _ =>
          submitters -> CompletionFromTransaction.acceptedCompletion(
            submitters = submitters.iterator
              .filter(internedParties)
              .map(stringInterning.party.unsafe.externalize)
              .toSet,
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            updateId = updateId,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset.map(ApiOffset.assertFromStringToLong),
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

  private def rejectedCommandParser(
      internedParties: Set[Int]
  ): RowParser[(Array[Int], CompletionStreamResponse)] =
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
            submitters = submitters.iterator
              .filter(internedParties)
              .map(stringInterning.party.unsafe.externalize)
              .toSet,
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            status = status,
            applicationId = applicationId,
            optSubmissionId = submissionId,
            optDeduplicationOffset = deduplicationOffset.map(ApiOffset.assertFromStringToLong),
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
            domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
            traceContext = traceContext,
          )
      }

  private def completionParser(
      internedParties: Set[Int]
  ): RowParser[(Array[Int], CompletionStreamResponse)] =
    acceptedCommandParser(internedParties) | rejectedCommandParser(internedParties)

  private val postPublishDataParser: RowParser[Option[PostPublishData]] =
    int("domain_id") ~
      str("message_uuid").? ~
      long("request_sequencer_counter").? ~
      long("record_time") ~
      str("application_id") ~
      str("command_id") ~
      array[Int]("submitters") ~
      offset("completion_offset") ~
      long("publication_time") ~
      str("submission_id").? ~
      str("update_id").? ~
      traceContextOption("trace_context")(noTracingLogger) ~
      bool("is_transaction") map {
        case internedDomainId ~ messageUuidString ~ requestSequencerCounterLong ~ recordTimeMicros ~ applicationId ~
            commandId ~ submitters ~ offset ~ publicationTimeMicros ~ submissionId ~ updateIdOpt ~ traceContext ~ true =>
          // note: we only collect completions for transactions here for acceptance and transactions and reassignments for rejection (is_transaction will be true in rejection reassignment case as well)
          Some(
            PostPublishData(
              submissionDomainId = stringInterning.domainId.externalize(internedDomainId),
              publishSource = messageUuidString
                .map(UUID.fromString)
                .map(PublishSource.Local(_): PublishSource)
                .getOrElse(
                  PublishSource.Sequencer(
                    requestSequencerCounter = SequencerCounter(
                      requestSequencerCounterLong
                        .getOrElse(
                          throw new IllegalStateException(
                            "if message_uuid is empty, this field should be populated"
                          )
                        )
                    ),
                    sequencerTimestamp = CantonTimestamp.ofEpochMicro(recordTimeMicros),
                  )
                ),
              applicationId = Ref.ApplicationId.assertFromString(applicationId),
              commandId = Ref.CommandId.assertFromString(commandId),
              actAs = submitters.view.map(stringInterning.party.externalize).toSet,
              offset = offset,
              publicationTime = CantonTimestamp.ofEpochMicro(publicationTimeMicros),
              submissionId = submissionId.map(Ref.SubmissionId.assertFromString),
              accepted = updateIdOpt.isDefined,
              traceContext = traceContext,
            )
          )
        case _ => None
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
      startExclusive: Offset,
      endInclusive: Offset,
  )(connection: Connection): Vector[PostPublishData] = {
    import ComposableQuery.*
    import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
    SQL"""
      SELECT
        domain_id,
        message_uuid,
        request_sequencer_counter,
        record_time,
        application_id,
        command_id,
        submitters,
        completion_offset,
        publication_time,
        submission_id,
        update_id,
        trace_context,
        is_transaction
      FROM
        lapi_command_completions
      WHERE
        ${QueryStrategy.offsetIsBetween(
        nonNullableColumn = "completion_offset",
        startExclusive = startExclusive,
        endInclusive = endInclusive,
      )}
      ORDER BY completion_offset ASC"""
      .asVectorOf(postPublishDataParser)(connection)
      .flatten
  }
}

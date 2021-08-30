// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, long, str}
import anorm.{~, RowParser}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.Conversions.{instant, offset}
import com.daml.platform.store.backend.CompletionStorageBackend
import com.google.protobuf.any
import com.google.rpc.status.{Status => StatusProto}

trait CompletionStorageBackendTemplate extends CompletionStorageBackend {

  def queryStrategy: QueryStrategy

  override def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Party],
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
          deduplication_time_seconds,
          deduplication_time_nanos,
          deduplication_start
        FROM
          participant_command_completions
        WHERE
          ($startExclusive is null or completion_offset > $startExclusive) AND
          completion_offset <= $endInclusive AND
          application_id = $applicationId AND
          ${queryStrategy.arrayIntersectionNonEmptyClause("submitters", parties)}
        ORDER BY completion_offset ASC"""
      .as(completionParser.*)(connection)
  }

  private val sharedColumns: RowParser[Offset ~ Instant ~ String ~ String ~ Option[String]] =
    offset("completion_offset") ~
      instant("record_time") ~
      str("command_id") ~
      str("application_id") ~
      str("submission_id").?

  private val acceptedCommandSharedColumns
      : RowParser[Offset ~ Instant ~ String ~ String ~ Option[String] ~ String] =
    sharedColumns ~ str("transaction_id")

  private val deduplicationOffsetColumn: RowParser[Option[String]] =
    str("deduplication_offset").?
  private val deduplicationTimeSecondsColumn: RowParser[Option[Long]] =
    long("deduplication_time_seconds").?
  private val deduplicationTimeNanosColumn: RowParser[Option[Int]] =
    int("deduplication_time_nanos").?
  private val deduplicationStartColumn: RowParser[Option[Instant]] =
    instant("deduplication_start").?

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    acceptedCommandSharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationTimeSecondsColumn ~ deduplicationTimeNanosColumn ~
      deduplicationStartColumn map {
        case offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~ transactionId ~
            deduplicationOffset ~ deduplicationTimeSeconds ~ deduplicationTimeNanos ~ _ =>
          CompletionFromTransaction.acceptedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            transactionId = transactionId,
            applicationId = applicationId,
            maybeSubmissionId = submissionId,
            maybeDeduplicationOffset = deduplicationOffset,
            maybeDeduplicationTimeSeconds = deduplicationTimeSeconds,
            maybeDeduplicationTimeNanos = deduplicationTimeNanos,
          )
      }

  private val rejectionStatusCodeColumn: RowParser[Int] = int("rejection_status_code")
  private val rejectionStatusMessageColumn: RowParser[String] = str("rejection_status_message")
  private val rejectionStatusDetailsColumn: RowParser[Option[InputStream]] =
    binaryStream("rejection_status_details").?

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~
      deduplicationOffsetColumn ~
      deduplicationTimeSecondsColumn ~ deduplicationTimeNanosColumn ~
      deduplicationStartColumn ~
      rejectionStatusCodeColumn ~
      rejectionStatusMessageColumn ~
      rejectionStatusDetailsColumn map {
        case offset ~ recordTime ~ commandId ~ applicationId ~ submissionId ~
            deduplicationOffset ~ deduplicationTimeSeconds ~ deduplicationTimeNanos ~ _ ~
            rejectionStatusCode ~ rejectionStatusMessage ~ rejectionStatusDetails =>
          val status =
            buildStatusProto(rejectionStatusCode, rejectionStatusMessage, rejectionStatusDetails)
          CompletionFromTransaction.rejectedCompletion(
            recordTime = recordTime,
            offset = offset,
            commandId = commandId,
            status = status,
            applicationId = applicationId,
            maybeSubmissionId = submissionId,
            maybeDeduplicationOffset = deduplicationOffset,
            maybeDeduplicationTimeSeconds = deduplicationTimeSeconds,
            maybeDeduplicationTimeNanos = deduplicationTimeNanos,
          )
      }

  private val completionParser: RowParser[CompletionStreamResponse] =
    acceptedCommandParser | rejectedCommandParser

  private def buildStatusProto(
      rejectionStatusCode: Int,
      rejectionStatusMessage: String,
      rejectionStatusDetails: Option[InputStream],
  ): StatusProto =
    StatusProto.of(
      rejectionStatusCode,
      rejectionStatusMessage,
      parseRejectionStatusDetails(rejectionStatusDetails),
    )

  private def parseRejectionStatusDetails(
      rejectionStatusDetails: Option[InputStream]
  ): Seq[any.Any] =
    rejectionStatusDetails
      .map(stream => StatusDetails.parseFrom(stream).details)
      .getOrElse(Seq.empty)
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, str}
import anorm.{RowParser, ~}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.Conversions.{instant, offset}
import com.daml.platform.store.backend.CompletionStorageBackend
import com.google.rpc.status.{Status => StatusProto}

trait CompletionStorageBackendTemplate extends CompletionStorageBackend {

  def queryStrategy: QueryStrategy

  private val sharedColumns: RowParser[Offset ~ Instant ~ String] =
    offset("completion_offset") ~ instant("record_time") ~ str("command_id")

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~ str("transaction_id") map {
      case offset ~ recordTime ~ commandId ~ transactionId =>
        CompletionFromTransaction.acceptedCompletion(recordTime, offset, commandId, transactionId)
    }

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~
      int("rejection_status_code") ~
      str("rejection_status_message") ~
      binaryStream("rejection_status_details").? map {
        case offset ~ recordTime ~ commandId ~
            rejectionStatusCode ~ rejectionStatusMessage ~ rejectionStatusDetails =>
          val details = rejectionStatusDetails
            .map(stream => StatusDetails.parseFrom(stream).details)
            .getOrElse(Seq.empty)
          val status = StatusProto.of(rejectionStatusCode, rejectionStatusMessage, details)
          CompletionFromTransaction.rejectedCompletion(recordTime, offset, commandId, status)
      }

  private val completionParser: RowParser[CompletionStreamResponse] =
    acceptedCommandParser | rejectedCommandParser

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
          rejection_status_details
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
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant

import anorm.{RowParser, ~}
import anorm.SqlParser.{int, str}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.platform.store.CompletionFromTransaction.toApiCheckpoint
import com.daml.platform.store.Conversions.{instant, offset}
import com.daml.platform.store.backend.CompletionStorageBackend
import com.google.rpc.status.Status

trait CompletionStorageBackendTemplate extends CompletionStorageBackend {

  def queryStrategy: QueryStrategy

  private val sharedCompletionColumns: RowParser[Offset ~ Instant ~ String] =
    offset("completion_offset") ~ instant("record_time") ~ str("command_id")

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    sharedCompletionColumns ~ str("transaction_id") map {
      case offset ~ recordTime ~ commandId ~ transactionId =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime, offset),
          completions = Seq(Completion(commandId, Some(Status()), transactionId)),
        )
    }

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedCompletionColumns ~ int("status_code") ~ str("status_message") map {
      case offset ~ recordTime ~ commandId ~ statusCode ~ statusMessage =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime, offset),
          completions = Seq(Completion(commandId, Some(Status(statusCode, statusMessage)))),
        )
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
          status_code,
          status_message
        FROM
          participant_command_completions
        WHERE
          completion_offset > $startExclusive AND
          completion_offset <= $endInclusive AND
          application_id = $applicationId AND
          ${queryStrategy.arrayIntersectionNonEmptyClause("submitters", parties)}
        ORDER BY completion_offset ASC"""
      .as(completionParser.*)(connection)
  }
}

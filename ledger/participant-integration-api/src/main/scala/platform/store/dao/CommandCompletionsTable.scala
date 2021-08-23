// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.SqlFunctions
import com.google.rpc.status.{Status => StatusProto}

private[platform] object CommandCompletionsTable {

  import SqlParser.{int, str}

  private val sharedColumns: RowParser[Offset ~ Instant ~ String] =
    offset("completion_offset") ~ instant("record_time") ~ str("command_id")

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~ str("transaction_id") map {
      case offset ~ recordTime ~ commandId ~ transactionId =>
        CompletionFromTransaction.acceptedCompletion(recordTime, offset, commandId, transactionId)
    }

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedColumns ~ int("status_code") ~ str("status_message") map {
      case offset ~ recordTime ~ commandId ~ statusCode ~ statusMessage =>
        val status = StatusProto.of(statusCode, statusMessage, Seq.empty)
        CompletionFromTransaction.rejectedCompletion(recordTime, offset, commandId, status)
    }

  val parser: RowParser[CompletionStreamResponse] = acceptedCommandParser | rejectedCommandParser

  def prepareGet(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
      sqlFunctions: SqlFunctions,
  ): SimpleSql[Row] = {
    val submittersInPartiesClause =
      sqlFunctions.arrayIntersectionWhereClause("submitters", parties)
    SQL"select completion_offset, record_time, command_id, transaction_id, status_code, status_message from participant_command_completions where ($startExclusive is null or completion_offset > $startExclusive) and completion_offset <= $endInclusive and application_id = $applicationId and #$submittersInPartiesClause order by completion_offset asc"
  }

  def prepareCompletionsDelete(endInclusive: Offset): SimpleSql[Row] =
    SQL"delete from participant_command_completions where completion_offset <= $endInclusive"
}

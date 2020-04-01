// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.platform.store.CompletionFromTransaction.{toApiCheckpoint, toErrorCode}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.entries.LedgerEntry
import com.google.rpc.status.Status

object CommandCompletionsTable {

  import SqlParser.{date, int, str}

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    offset("completion_offset") ~ date("record_time") ~ str("command_id") ~ str("transaction_id") map {
      case offset ~ recordTime ~ commandId ~ transactionId =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime.toInstant, offset),
          completions = Seq(Completion(commandId, Some(Status()), transactionId)))
    }

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    offset("completion_offset") ~ date("record_time") ~ str("command_id") ~ int("status_code") ~ str(
      "status_message") map {
      case offset ~ recordTime ~ commandId ~ statusCode ~ statusMessage =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime.toInstant, offset),
          completions = Seq(Completion(commandId, Some(Status(statusCode, statusMessage)))))
    }

  private val checkpointParser: RowParser[CompletionStreamResponse] =
    offset("completion_offset") ~ date("record_time") map {
      case offset ~ recordTime =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime.toInstant, offset),
          completions = Seq())
    }

  val parser: RowParser[CompletionStreamResponse] =
    acceptedCommandParser | rejectedCommandParser | checkpointParser

  // TODO The query has to account for checkpoint, which is why it
  // TODO returns rows there the application_id and submitting_party
  // TODO are null. Remove as soon as checkpoints are gone.
  def prepareGet(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): SimpleSql[Row] =
    SQL"""select
            completion_offset,
            record_time,
            application_id,
            submitting_party,
            command_id,
            transaction_id,
            status_code,
            status_message
          from participant_command_completions
          where
            completion_offset > $startExclusive and completion_offset <= $endInclusive and
            (
              (application_id is null and submitting_party is null)
              or
              (application_id = ${applicationId: String} and submitting_party in (${parties
      .asInstanceOf[Set[String]]}))
            )
          order by completion_offset asc"""

  // The insert will be prepared only if this entry contains all the information
  // necessary to be rendered as part of the completion service
  def prepareInsert(offset: Offset, entry: LedgerEntry): Option[SimpleSql[Row]] =
    entry match {
      case LedgerEntry.Transaction(
          Some(cmdId),
          txId,
          Some(appId),
          Some(submitter),
          _,
          _,
          recordTime,
          _,
          _) =>
        Some(
          SQL"""insert into participant_command_completions(completion_offset, record_time, application_id, submitting_party, command_id, transaction_id)
                values ($offset, $recordTime, ${appId: String}, ${submitter: String}, ${cmdId: String}, ${txId: String})""")
      case LedgerEntry.Rejection(recordTime, cmdId, appId, submitter, reason) =>
        val code = toErrorCode(reason).value()
        Some(
          SQL"""insert into participant_command_completions(completion_offset, record_time, application_id, submitting_party, command_id, status_code, status_message)
                values ($offset, $recordTime, ${appId: String}, ${submitter: String}, ${cmdId: String}, $code, ${reason.description})""")
      case _ =>
        None
    }

}

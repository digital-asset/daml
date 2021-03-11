// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.{QueryNonPruned, SqlFunctions}
import com.daml.platform.store.dao.{CommandCompletionsTable, DbDispatcher}

import scala.concurrent.{ExecutionContext, Future}

class JdbcCompletionsDao(
    dispatcher: DbDispatcher,
    dbType: DbType,
    metrics: Metrics,
    executionContext: ExecutionContext,
) extends CompletionsDao {

  private val sqlFunctions = SqlFunctions(dbType)

  override def getFilteredCompletions(
      range: Range,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[List[(Offset, CompletionStreamResponse)]] = {
    val query = CommandCompletionsTable.prepareGet(
      startExclusive = range.startExclusive,
      endInclusive = range.endInclusive,
      applicationId = applicationId,
      parties = parties,
      sqlFunctions = sqlFunctions,
    )
    dispatcher
      .executeSql(metrics.daml.index.db.getCompletions) { implicit connection =>
        QueryNonPruned.executeSql[List[CompletionStreamResponse]](
          query.as(CommandCompletionsTable.parser.*),
          range.startExclusive,
          pruned =>
            s"Command completions request from ${range.startExclusive.toHexString} to ${range.endInclusive.toHexString} overlaps with pruned offset ${pruned.toHexString}",
        )
      }
      .flatMap(eitherToFuture)(executionContext)
      .map(_.map(response => offsetFor(response) -> response))(executionContext)
  }

  private def offsetFor(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)

  override def getAllCompletions(range: Range)(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CommandCompletionsTable.CompletionStreamResponseWithParties)]] = {
    val query = CommandCompletionsTable.getStmtForAllParties(
      startExclusive = range.startExclusive,
      endInclusive = range.endInclusive,
    )
    dispatcher
      .executeSql(metrics.daml.index.db.getCompletionsForAllParties) { implicit connection =>
        QueryNonPruned
          .executeSql[List[CommandCompletionsTable.CompletionStreamResponseWithParties]](
            query.as(CommandCompletionsTable.parserWithParties.*),
            range.startExclusive,
            pruned =>
              s"Command completions request from ${range.startExclusive.toHexString} to ${range.endInclusive.toHexString} overlaps with pruned offset ${pruned.toHexString}",
          )
      }
      .flatMap(eitherToFuture)(executionContext)
      .map(_.map(response => offsetFor(response.completion) -> response))(executionContext)
  }

  private def eitherToFuture[T](dbQueryResult: Either[Throwable, T]): Future[T] =
    Future.fromTry(dbQueryResult.toTry)
}

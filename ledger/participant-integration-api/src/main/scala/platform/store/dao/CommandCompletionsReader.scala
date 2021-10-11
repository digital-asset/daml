// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.{QueryNonPruned, SqlFunctions}

import scala.concurrent.{ExecutionContext, Future}

private[dao] final class CommandCompletionsReader(
    dispatcher: DbDispatcher,
    dbType: DbType,
    metrics: Metrics,
    executionContext: ExecutionContext,
) extends LedgerDaoCommandCompletionsReader {

  private val sqlFunctions = SqlFunctions(dbType)

  private def offsetFor(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)

  override def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val query = CommandCompletionsTable.prepareGet(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      applicationId = applicationId,
      parties = parties,
      sqlFunctions = sqlFunctions,
    )
    Source
      .future(
        dispatcher
          .executeSql(metrics.daml.index.db.getCompletions) { implicit connection =>
            QueryNonPruned.executeSql[List[CompletionStreamResponse]](
              query.as(CommandCompletionsTable.parser.*),
              startExclusive,
              pruned =>
                s"Command completions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} overlaps with pruned offset ${pruned.toHexString}",
            )
          }
          .flatMap(_.fold(Future.failed, Future.successful))(executionContext)
      )
      .mapConcat(_.map(response => offsetFor(response) -> response))
  }

}

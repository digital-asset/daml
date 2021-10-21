// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.store.appendonlydao.events.QueryNonPruned
import com.daml.platform.store.backend.CompletionStorageBackend

private[appendonlydao] final class CommandCompletionsReader(
    dispatcher: DbDispatcher,
    storageBackend: CompletionStorageBackend,
    queryNonPruned: QueryNonPruned,
    metrics: Metrics,
) extends LedgerDaoCommandCompletionsReader {

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
    Source
      .future(
        dispatcher
          .executeSql(metrics.daml.index.db.getCompletions) { implicit connection =>
            queryNonPruned.executeSql[List[CompletionStreamResponse]](
              storageBackend.commandCompletions(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                applicationId = applicationId,
                parties = parties,
              )(connection),
              startExclusive,
              pruned =>
                s"Command completions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} overlaps with pruned offset ${pruned.toHexString}",
            )
          }
      )
      .mapConcat(_.map(response => offsetFor(response) -> response))
  }

}

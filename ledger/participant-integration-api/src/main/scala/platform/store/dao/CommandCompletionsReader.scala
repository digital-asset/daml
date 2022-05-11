// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.{ApiOffset, ApplicationId, Party}
import com.daml.platform.store.dao.events.QueryNonPruned
import com.daml.platform.store.backend.CompletionStorageBackend

private[dao] final class CommandCompletionsReader(
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
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    Source
      .future(
        dispatcher
          .executeSql(metrics.daml.index.db.getCompletions) { implicit connection =>
            queryNonPruned.executeSql[List[CompletionStreamResponse]](
              query = storageBackend.commandCompletions(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                applicationId = applicationId,
                parties = parties,
              )(connection),
              minOffsetExclusive = startExclusive,
              error = pruned =>
                s"Command completions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} overlaps with pruned offset ${pruned.toHexString}",
            )
          }
      )
      .mapConcat(_.map(response => offsetFor(response) -> response))
  }

}

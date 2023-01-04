// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.Connection

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.{ApiOffset, ApplicationId, Party}
import com.daml.platform.store.dao.events.QueryNonPruned
import com.daml.platform.store.backend.CompletionStorageBackend

/** @param pageSize a single DB fetch query is guaranteed to fetch no more than this many results.
  */
private[dao] final class CommandCompletionsReader(
    dispatcher: DbDispatcher,
    storageBackend: CompletionStorageBackend,
    queryNonPruned: QueryNonPruned,
    metrics: Metrics,
    pageSize: Int,
) extends LedgerDaoCommandCompletionsReader {

  private def offsetFor(response: CompletionStreamResponse): Offset = {
    // It would nice to obtain the offset such that it's obvious that it always exists (rather then relaying on calling .get)
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)
  }

  override def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val pruneSafeQuery =
      (range: QueryRange[Offset]) => { implicit connection: Connection =>
        queryNonPruned.executeSql[Vector[CompletionStreamResponse]](
          query = storageBackend.commandCompletions(
            startExclusive = range.startExclusive,
            endInclusive = range.endInclusive,
            applicationId = applicationId,
            parties = parties,
            limit = pageSize,
          )(connection),
          minOffsetExclusive = startExclusive,
          error = (prunedUpTo: Offset) =>
            s"Command completions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} overlaps with pruned offset ${prunedUpTo.toHexString}",
        )
      }

    val initialRange = new QueryRange[Offset](
      startExclusive = startExclusive,
      endInclusive = endInclusive,
    )
    val source: Source[CompletionStreamResponse, NotUsed] = PaginatingAsyncStream
      .streamFromSeekPagination[QueryRange[Offset], CompletionStreamResponse](
        startFromOffset = initialRange,
        getOffset = (previousCompletion: CompletionStreamResponse) => {
          val lastOffset = offsetFor(previousCompletion)
          initialRange.copy(startExclusive = lastOffset)
        },
      ) { subRange: QueryRange[Offset] =>
        dispatcher.executeSql(metrics.daml.index.db.main.getCompletions)(pruneSafeQuery(subRange))
      }
    source.map(response => offsetFor(response) -> response)
  }

}

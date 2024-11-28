// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.CompletionStorageBackend
import com.digitalasset.canton.platform.store.dao.events.QueryValidRange
import com.digitalasset.canton.platform.{ApplicationId, Party}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection

/** @param pageSize a single DB fetch query is guaranteed to fetch no more than this many results.
  */
private[dao] final class CommandCompletionsReader(
    dispatcher: DbDispatcher,
    storageBackend: CompletionStorageBackend,
    queryValidRange: QueryValidRange,
    metrics: LedgerApiServerMetrics,
    pageSize: Int,
    override protected val loggerFactory: NamedLoggerFactory,
) extends LedgerDaoCommandCompletionsReader
    with NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def offsetFor(response: CompletionStreamResponse): Offset =
    // It would be nice to obtain the offset such that it's obvious that it always exists (rather then relaying on calling .get)
    Offset.tryFromLong(response.completionResponse.completion.get.offset)

  override def getCommandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val pruneSafeQuery =
      (range: QueryRange[Offset]) => { implicit connection: Connection =>
        queryValidRange.withRangeNotPruned[Vector[CompletionStreamResponse]](
          minOffsetInclusive = startInclusive,
          maxOffsetInclusive = endInclusive,
          errorPruning = (prunedOffset: Offset) =>
            s"Command completions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} overlaps with pruned offset ${prunedOffset.unwrap}",
          errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
            s"Command completions request from ${startInclusive.unwrap} to ${endInclusive.unwrap} is beyond ledger end offset ${ledgerEndOffset
                .fold(0L)(_.unwrap)}",
        ) {
          storageBackend.commandCompletions(
            startInclusive = range.startInclusive,
            endInclusive = range.endInclusive,
            applicationId = applicationId,
            parties = parties,
            limit = pageSize,
          )(connection)
        }
      }

    val initialRange = new QueryRange[Offset](
      startInclusive = startInclusive,
      endInclusive = endInclusive,
    )
    val source: Source[CompletionStreamResponse, NotUsed] = paginatingAsyncStream
      .streamFromSeekPagination[QueryRange[Offset], CompletionStreamResponse](
        startFromOffset = initialRange,
        getOffset = (previousCompletion: CompletionStreamResponse) => {
          val lastOffset = offsetFor(previousCompletion)
          initialRange.copy(startInclusive = lastOffset.increment)
        },
      ) { (subRange: QueryRange[Offset]) =>
        dispatcher.executeSql(metrics.index.db.getCompletions)(pruneSafeQuery(subRange))
      }
    source.map(response => offsetFor(response) -> response)
  }
}

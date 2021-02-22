package com.daml.platform.store

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.LedgerReadDao

import scala.concurrent.ExecutionContext

class CompletionsReaderWithCache(ledgerDao: LedgerReadDao, maxItems: Int) {

  private val cache = new CompletionsCache(maxItems)

  def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = { // if is historic not synced, else synced
    val cachedCompletions = cache.get(startExclusive, endInclusive, applicationId, parties) // synchronized
    val historicCompletions =
      fetchHistoric(cachedCompletions, startExclusive, endInclusive, applicationId, parties)
    val futureCompletions =
      fetchFuture(cachedCompletions, startExclusive, endInclusive, applicationId, parties) // at least partially synchronized
    historicCompletions
      .concat(Source(cachedCompletions.cache.toList))
      .concat(futureCompletions)
  }

  private def fetchHistoric(
      cachedCompletions: Cache,
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    cachedCompletions.boundaries
      .map { boundaries =>
        if (boundaries.startExclusive <= startExclusive) {
          Source.empty[(Offset, CompletionStreamResponse)]
        } else {
          val historicEnd =
            if (boundaries.startExclusive > endInclusive) endInclusive
            else boundaries.startExclusive
          ledgerDao.completions
            .getCommandCompletions(startExclusive, historicEnd, applicationId, parties)
        }
      }
      .getOrElse(Source.empty[(Offset, CompletionStreamResponse)])
  }

  private def fetchFuture(
      cachedCompletions: Cache,
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val boundaries =
      cachedCompletions.boundaries.getOrElse(Boundaries(startExclusive, endInclusive))
    if (cachedCompletions.boundaries.isDefined && boundaries.endInclusive >= endInclusive) {
      Source.empty[(Offset, CompletionStreamResponse)]
    } else {
      val futureStart =
        if (cachedCompletions.boundaries.isDefined && boundaries.endInclusive > startExclusive)
          boundaries.endInclusive
        else startExclusive
      val futureCompletions =
        ledgerDao.completions.getAllCommandCompletions(futureStart, endInclusive)
      futureCompletions.foreach(cache.set(futureStart, endInclusive, _))
      Source
        .future(futureCompletions)
        .mapConcat(_.filter { case (_, completionsWithParties) =>
          completionsWithParties.applicationId == applicationId && completionsWithParties.parties
            .exists(parties.contains)
        }.map { case (offset, completionsWithParties) =>
          (offset, completionsWithParties.completion)
        })
    }
  }
}

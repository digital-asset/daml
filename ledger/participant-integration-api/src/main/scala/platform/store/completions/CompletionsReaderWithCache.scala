package com.daml.platform.store.completions

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.completions.CompletionsCache.{Boundaries, Cache}

import scala.concurrent.ExecutionContext

/**
 * Responsible for reading completions from data store. Caches maxItems newest completions to decrease database usage
 * @param completionsDao data access object for completions
 * @param maxItems maximum amount of in memory cached completions
 */
class CompletionsReaderWithCache(completionsDao: CompletionsDao, maxItems: Int) {

  private val cache = new CompletionsCache(maxItems)

  /**
   * Returns completions stream based on parameters. Completions are read from cache or if not available from database.
   * Newest completions read from database are added to cache
   * @param startExclusive
   * @param endInclusive
   * @param applicationId
   * @param parties
   * @param loggingContext
   * @param executionContext
   * @return
   */
  def getCompletionsPage(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = { // should be future of seq
    val cachedCompletions = synchronized(
      cache.getCachedCompletions(startExclusive, endInclusive, applicationId, parties)
    )
    val historicCompletions =
      fetchHistoric(cachedCompletions, startExclusive, endInclusive, applicationId, parties)
    val futureCompletions =
      fetchFuture(
        cachedCompletions,
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      ) // at least partially synchronized
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
          completionsDao
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
        completionsDao.getAllCommandCompletions(futureStart, endInclusive) // we should block here
      futureCompletions.foreach(synchronized(cache.cacheCompletions(futureStart, endInclusive, _)))
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

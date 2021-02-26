package com.daml.platform.store.completions
import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties

import scala.collection.SortedMap
import scala.concurrent.{ExecutionContext, Future}

class PagedCompletionsReaderWithCache(completionsDao: CompletionsDao, maxItems: Int)
    extends PagedCompletionsReader {

  private val cache: AtomicReference[RangeCache[CompletionStreamResponseWithParties]] = new AtomicReference(
    RangeCache.empty[CompletionStreamResponseWithParties](maxItems))


  private val pendingRequest: AtomicReference[Future[Unit]] =
    new AtomicReference[Future[Unit]](Future.unit)

  /** Returns completions stream filtered based on parameters.
    * Newest completions read from database are added to cache
    *
    * @param startExclusive
    * @param endInclusive
    * @param applicationId
    * @param parties
    * @param loggingContext
    * @param executionContext
    * @return
    */
  override def getCompletionsPage(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val cachedCompletions = cache.get().slice(startExclusive, endInclusive)
    val historicCompletionsFuture =
      fetchHistoric(
        cachedCompletions.map(_.boundaries),
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      )
    val futureCompletionsFuture =
      fetchFuture(
        cachedCompletions.map(_.boundaries),
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      ) // at least partially synchronized
    for {
      historicCompletions <- historicCompletionsFuture
      futureCompletions <- futureCompletionsFuture
    } yield historicCompletions ++ filterCache(
      cachedCompletions,
      applicationId,
      parties,
    ) ++ futureCompletions
  }

  private def filterCache(
      cacheOpt: Option[RangeCache[CompletionStreamResponseWithParties]],
      applicationId: ApplicationId,
      parties: Set[Party],
  ): Seq[(Offset, CompletionStreamResponse)] = {
    cacheOpt
      .map(
        _.cache.toSeq
          .filter { case (_, completion) =>
            completion.applicationId == applicationId && completion.parties.exists(parties.contains)
          }
          .map { case (offset, completionWithParties) =>
            (offset, completionWithParties.completion)
          }
      )
      .getOrElse(Nil)
  }

  private def fetchHistoric(
      cachedBoundaries: Option[Boundaries],
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val boundariesToFetch =
      cachedBoundaries.fold[Option[Boundaries]](Some(Boundaries(startExclusive, endInclusive))) {
        cached =>
          if (cached.startExclusive <= startExclusive) {
            None
          } else {
            Some(
              Boundaries(
                startExclusive = startExclusive,
                endInclusive =
                  if (cached.startExclusive > endInclusive) endInclusive else cached.startExclusive,
              )
            )
          }
      }
    boundariesToFetch
      .map(boundaries =>
        completionsDao
          .getFilteredCompletions(
            boundaries.startExclusive,
            boundaries.endInclusive,
            applicationId,
            parties,
          )
      )
      .getOrElse(Future.successful(Nil))
  }

  private def fetchFuture(
      cachedBoundaries: Option[Boundaries],
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val boundariesToFetch =
      cachedBoundaries.fold[Option[Boundaries]](Some(Boundaries(startExclusive, endInclusive))) {
        cached =>
          if (cached.endInclusive >= endInclusive) {
            None
          } else {
            Some(
              Boundaries(
                startExclusive =
                  if (cached.endInclusive > startExclusive) cached.endInclusive else startExclusive,
                endInclusive = endInclusive,
              )
            )
          }
      }

    boundariesToFetch.map { boundaries =>
      synchronized {
        val request = pendingRequest.get()
        if (request.isCompleted) {
          val allCompletionsFuture =
            completionsDao.getAllCompletions(boundaries.startExclusive, boundaries.endInclusive)
          val updateCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
          val cacheToUpdate = if(boundaries.endInclusive < startExclusive) RangeCache.empty[CompletionStreamResponseWithParties](maxItems) else cache.get()
            val updatedCache = cacheToUpdate.cache(
              boundaries.startExclusive,
              boundaries.endInclusive,
              SortedMap(fetchedCompletions: _*),
            )
            cache.set(updatedCache)
          }
          pendingRequest.set(updateCacheRequest)
          allCompletionsFuture.map(_.filter { case (_, completion) =>
            completion.applicationId == applicationId && completion.parties.exists(parties.contains)
          }.map {case (offset, completionWithParties) => (offset, completionWithParties.completion)}) // convert to result set
          // create pending request
        } else {
          request.flatMap { _ =>
            getCompletionsPage(boundaries.startExclusive, boundaries.endInclusive, applicationId, parties)
          }
        }
      }
    }.getOrElse(Future.successful(Nil))

  }
}

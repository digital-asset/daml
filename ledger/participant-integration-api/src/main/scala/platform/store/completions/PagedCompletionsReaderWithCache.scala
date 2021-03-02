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

  private val cacheRef: AtomicReference[RangeCache[CompletionStreamResponseWithParties]] =
    new AtomicReference(RangeCache.empty[CompletionStreamResponseWithParties](maxItems))

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
    val inMemCache = cacheRef.get()
    val historicCompletionsFuture =
      fetchHistoric(
        inMemCache.boundaries,
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      )
    val futureCompletionsFuture =
      fetchFuture(
        inMemCache,
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      ) // at least partially synchronized
    for {
      historicCompletions <- historicCompletionsFuture
      futureCompletions <- futureCompletionsFuture
    } yield historicCompletions ++ filterCache(
      inMemCache.slice(startExclusive, endInclusive),
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
      cachedBoundaries: Boundaries,
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val boundariesToFetch =
      if (
        cachedBoundaries.startExclusive <= startExclusive || cachedBoundaries.isOffsetBeforeBegin()
      ) {
        None
      } else {
        Some(
          Boundaries(
            startExclusive = startExclusive,
            endInclusive =
              if (cachedBoundaries.startExclusive > endInclusive) endInclusive
              else cachedBoundaries.startExclusive,
          )
        )
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
      inMemCache: RangeCache[CompletionStreamResponseWithParties],
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val boundariesToFetch =
      if (inMemCache.boundaries.endInclusive >= endInclusive) {
        None
      } else {
        Some(
          Boundaries(
            startExclusive =
              if (inMemCache.boundaries.endInclusive > startExclusive)
                inMemCache.boundaries.endInclusive
              else startExclusive,
            endInclusive = endInclusive,
          )
        )
      }

    boundariesToFetch
      .map { boundaries =>
        synchronized {
          val request = pendingRequest.get()
          if (request.isCompleted) {
            val allCompletionsFuture =
              completionsDao.getAllCompletions(boundaries.startExclusive, boundaries.endInclusive)
            val updateCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
              val cacheToUpdate =
                if (boundaries.endInclusive < startExclusive)
                  RangeCache.empty[CompletionStreamResponseWithParties](maxItems)
                else cacheRef.get()
              val updatedCache = cacheToUpdate.cache(
                boundaries.startExclusive,
                boundaries.endInclusive,
                SortedMap(fetchedCompletions: _*),
              )
              cacheRef.set(updatedCache)
            }
            pendingRequest.set(updateCacheRequest)
            allCompletionsFuture.map(_.filter { case (_, completion) =>
              completion.applicationId == applicationId && completion.parties.exists(
                parties.contains
              )
            }.map { case (offset, completionWithParties) =>
              (offset, completionWithParties.completion)
            })
          } else {
            request.flatMap { _ =>
              getCompletionsPage(
                boundaries.startExclusive,
                boundaries.endInclusive,
                applicationId,
                parties,
              )
            }
          }
        }
      }
      .getOrElse(Future.successful(Nil))

  }
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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

/** Completions reader implementation that caches maxItems of newest completions in memory.
  * Under concurrent access, requests that miss the cache will be fetching data from database sequentially -
  * only one request at the time can be executed on database for completions newer than last cached offset.
  * @param completionsDao DAO object responsible for fetching completions from datastore
  * @param maxItems maximum amount of completions stored in in-memory cache
  * @param executionContext context in which are run async operations
  */
class PagedCompletionsReaderWithCache(completionsDao: CompletionsDao, maxItems: Int)(implicit
    executionContext: ExecutionContext
) extends PagedCompletionsReader {

  private val cacheRef: AtomicReference[RangeCache[CompletionStreamResponseWithParties]] =
    new AtomicReference(RangeCache.empty[CompletionStreamResponseWithParties](maxItems))

  private val pendingRequestRef: AtomicReference[Future[Unit]] =
    new AtomicReference[Future[Unit]](Future.unit)

  /** Returns completions stream filtered based on parameters.
    * Newest completions read from database are added to cache
    *
    * @param startExclusive start offset
    * @param endInclusive end offset
    * @param applicationId completions application id
    * @param parties required completions submitters
    * @param loggingContext logging context
    * @return
    */
  override def getCompletionsPage(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val inMemCache = cacheRef.get()
    val historicCompletionsFuture =
      calculateHistoricRangeToFetch(inMemCache.range, startExclusive, endInclusive)
        .map(historicRangeToFetch =>
          fetchHistoric(
            historicRangeToFetch.startExclusive,
            historicRangeToFetch.endInclusive,
            applicationId,
            parties,
          )
        )
        .getOrElse(futureEmptyList)

    val futureCompletionsFuture =
      calculateFutureRangeToFetch(inMemCache.range, startExclusive, endInclusive)
        .map(futureRangeToFetch =>
          fetchFuture(
            inMemCache,
            futureRangeToFetch.startExclusive,
            futureRangeToFetch.endInclusive,
            applicationId,
            parties,
          )
        )
        .getOrElse(futureEmptyList)

    val filteredCache = inMemCache
      .slice(startExclusive, endInclusive)
      .map(cache => filterAndMapToResponse(cache.cache.toSeq, applicationId, parties))
      .getOrElse(Seq.empty)
    for {
      historicCompletions <- historicCompletionsFuture
      futureCompletions <- futureCompletionsFuture
    } yield historicCompletions ++ filteredCache ++ futureCompletions
  }

  /** fetches completions older than start offset of cache
    */
  private def fetchHistoric(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] =
    completionsDao
      .getFilteredCompletions(
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      )

  private def calculateHistoricRangeToFetch(
      cachedRange: Range,
      startExclusive: Offset,
      endInclusive: Offset,
  ): Option[Range] = Range(startExclusive, endInclusive).lesserRangeDifference(cachedRange)

  /** fetches completions ahead cache and caches results
    */
  private def fetchFuture(
      inMemCache: RangeCache[CompletionStreamResponseWithParties],
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]] =
    synchronized {
      val pendingRequest = pendingRequestRef.get()
      if (pendingRequest.isCompleted) {
        val allCompletionsFuture =
          createAndSetNewPendingRequest(
            startExclusive,
            endInclusive,
            inMemCache.range.startExclusive,
          )
        allCompletionsFuture.map(filterAndMapToResponse(_, applicationId, parties))
      } else {
        pendingRequest.flatMap { _ =>
          getCompletionsPage(
            startExclusive,
            endInclusive,
            applicationId,
            parties,
          )
        }
      }
    }

  private def filterAndMapToResponse(
      completions: Seq[(Offset, CompletionStreamResponseWithParties)],
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  ): Seq[(Offset, CompletionStreamResponse)] =
    completions
      .filter { case (_, completion) =>
        completion.applicationId == applicationId && completion.parties.exists(
          parties.contains
        )
      }
      .map { case (offset, completionWithParties) =>
        (offset, completionWithParties.completion)
      }

  private def createAndSetNewPendingRequest(
      startExclusive: Offset,
      endInclusive: Offset,
      cachedStartExclusive: Offset,
  )(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]] = {
    val allCompletionsFuture =
      completionsDao.getAllCompletions(startExclusive, endInclusive)
    val updateCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
      val cacheToUpdate =
        if (startExclusive > cachedStartExclusive)
          RangeCache.empty[CompletionStreamResponseWithParties](maxItems)
        else cacheRef.get()
      val updatedCache = cacheToUpdate.cache(
        startExclusive,
        endInclusive,
        SortedMap(fetchedCompletions: _*),
      )
      cacheRef.set(updatedCache)
    }
    pendingRequestRef.set(updateCacheRequest)
    allCompletionsFuture
  }

  private def calculateFutureRangeToFetch(
      cachedRange: Range,
      startExclusive: Offset,
      endInclusive: Offset,
  ): Option[Range] = Range(startExclusive, endInclusive).greaterRangeDifference(cachedRange)

  private val futureEmptyList = Future.successful(Nil)
}

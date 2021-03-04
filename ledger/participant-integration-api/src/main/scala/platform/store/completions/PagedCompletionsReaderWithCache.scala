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

/** Completions reader implementation that caches maxItems of newest completions in memory
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
      )

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
      cachedBoundaries: Boundaries,
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    calculateHistoricBoundariesToFetch(cachedBoundaries, startExclusive, endInclusive)
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

  private def calculateHistoricBoundariesToFetch(
      cachedBoundaries: Boundaries,
      startExclusive: Offset,
      endInclusive: Offset,
  ): Option[Boundaries] =
    if (cachedBoundaries.startExclusive <= startExclusive || cachedBoundaries.isBeforeBegin) {
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
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    calculateFutureBoundariesToFetch(inMemCache.boundaries, startExclusive, endInclusive)
      .map { boundariesToFetch =>
        synchronized {
          val pendingRequest = pendingRequestRef.get()
          if (pendingRequest.isCompleted) {
            val allCompletionsFuture =
              createAndSetNewPendingRequest(boundariesToFetch, inMemCache.boundaries.startExclusive)
            allCompletionsFuture.map(filterAndMapToResponse(_, applicationId, parties))
          } else {
            pendingRequest.flatMap { _ =>
              getCompletionsPage(
                boundariesToFetch.startExclusive,
                boundariesToFetch.endInclusive,
                applicationId,
                parties,
              )
            }
          }
        }
      }
      .getOrElse(Future.successful(Nil))
  }

  private def filterAndMapToResponse(
      completions: Seq[(Offset, CompletionStreamResponseWithParties)],
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  ): Seq[(Offset, CompletionStreamResponse)] = {
    completions
      .filter { case (_, completion) =>
        completion.applicationId == applicationId && completion.parties.exists(
          parties.contains
        )
      }
      .map { case (offset, completionWithParties) =>
        (offset, completionWithParties.completion)
      }
  }

  private def createAndSetNewPendingRequest(boundaries: Boundaries, cachedStartExclusive: Offset)(
      implicit loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]] = {
    val allCompletionsFuture =
      completionsDao.getAllCompletions(boundaries.startExclusive, boundaries.endInclusive)
    val updateCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
      val cacheToUpdate =
        if (boundaries.startExclusive > cachedStartExclusive)
          RangeCache.empty[CompletionStreamResponseWithParties](maxItems)
        else cacheRef.get()
      val updatedCache = cacheToUpdate.cache(
        boundaries.startExclusive,
        boundaries.endInclusive,
        SortedMap(fetchedCompletions: _*),
      )
      cacheRef.set(updatedCache)
    }
    pendingRequestRef.set(updateCacheRequest)
    allCompletionsFuture
  }

  private def calculateFutureBoundariesToFetch(
      cachedBoundaries: Boundaries,
      startExclusive: Offset,
      endInclusive: Offset,
  ): Option[Boundaries] = if (cachedBoundaries.endInclusive >= endInclusive) {
    None
  } else {
    Some(
      Boundaries(
        startExclusive =
          if (cachedBoundaries.endInclusive > startExclusive)
            cachedBoundaries.endInclusive
          else startExclusive,
        endInclusive = endInclusive,
      )
    )
  }
}

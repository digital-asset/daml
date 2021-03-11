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

  protected val cacheRef: AtomicReference[RangeCache[CompletionStreamResponseWithParties]] =
    new AtomicReference(RangeCache.empty[CompletionStreamResponseWithParties](maxItems))

  private val pendingRequestRef: AtomicReference[Future[Unit]] =
    new AtomicReference[Future[Unit]](Future.unit)

  /** Returns completions stream filtered based on parameters.
    * Newest completions read from database are added to cache
    *
    * @param requestedRange requested offsets range
    * @param applicationId completions application id
    * @param parties required completions submitters
    * @param loggingContext logging context
    * @return
    */
  override def getCompletionsPage(
      requestedRange: Range,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] = {
    val inMemCache = cacheRef.get()
    val historicCompletionsFuture =
      calculateHistoricRangeToFetch(inMemCache.range, requestedRange)
        .map(fetchHistoric(_, applicationId, parties))
        .getOrElse(futureEmptyList)

    val futureCompletionsFuture =
      calculateFutureRangeToFetch(inMemCache.range, requestedRange)
        .map(fetchFuture(_, applicationId, parties))
        .getOrElse(futureEmptyList)

    val filteredCache = inMemCache
      .slice(requestedRange)
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
      range: Range,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] =
    completionsDao.getFilteredCompletions(range, applicationId, parties)

  private def calculateHistoricRangeToFetch(
      cachedRange: Range,
      requestedRange: Range,
  ): Option[Range] = requestedRange.lesserRangeDifference(cachedRange)

  /** fetches completions ahead cache and caches results
    */
  private def fetchFuture(
      rangeToFetch: Range,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]] =
    pendingRequestRef.synchronized {
      val pendingRequest = pendingRequestRef.get()
      if (pendingRequest.isCompleted) {
        val allCompletionsFuture = createAndSetNewPendingRequest(rangeToFetch)
        allCompletionsFuture.map(filterAndMapToResponse(_, applicationId, parties))
      } else {
        pendingRequest.flatMap { _ =>
          getCompletionsPage(rangeToFetch, applicationId, parties)
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
      requestedRange: Range
  )(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]] = {
    val allCompletionsFuture =
      completionsDao.getAllCompletions(requestedRange)
    val updateCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
      cacheRef.updateAndGet { cache =>
        if (requestedRange.startExclusive > cache.range.endInclusive) {
          RangeCache(requestedRange, maxItems, SortedMap(fetchedCompletions: _*))
        } else {
          cache.append(requestedRange, SortedMap(fetchedCompletions: _*))
        }
      }
      ()
    }
    pendingRequestRef.set(updateCacheRequest)
    updateCacheRequest.flatMap(_ => allCompletionsFuture)
  }

  private def calculateFutureRangeToFetch(
      cachedRange: Range,
      requestedRange: Range,
  ): Option[Range] = requestedRange.greaterRangeDifference(cachedRange)

  private val futureEmptyList = Future.successful(Nil)
}

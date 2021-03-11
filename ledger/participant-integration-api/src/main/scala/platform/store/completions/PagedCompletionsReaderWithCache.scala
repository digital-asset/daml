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

/** Paged completions reader implementation that caches maxItems of newest completions in memory.
  * Under concurrent access, requests that miss the cache will be fetching data from database sequentially -
  * only one request at the time can be executed on database for completions newer than the last cached offset.
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
    val cache = cacheRef.get()
    val historicCompletionsFuture =
      requestedRange
        .lesserRangeDifference(cache.range) // calculate historic range to fetch
        .map(historicRange => fetchHistoric(historicRange, applicationId, parties))
        .getOrElse(futureEmptyList)

    val futureCompletionsFuture = requestedRange
      .greaterRangeDifference(cache.range) // calculate future range to fetch
      .map(futureRange => fetchFuture(futureRange, applicationId, parties))
      .getOrElse(futureEmptyList)

    val cachedCompletions = cache
      .slice(requestedRange)
      .map(slicedCache => filterAndMapToResponse(slicedCache.cache.toSeq, applicationId, parties))
      .getOrElse(Seq.empty)
    for {
      historicCompletions <- historicCompletionsFuture
      futureCompletions <- futureCompletionsFuture
    } yield historicCompletions ++ cachedCompletions ++ futureCompletions
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
    val updatedCacheRequest = allCompletionsFuture.map { fetchedCompletions =>
      cacheRef.updateAndGet(_.append(requestedRange, SortedMap(fetchedCompletions: _*)))
      ()
    }
    pendingRequestRef.set(updatedCacheRequest)
    updatedCacheRequest.flatMap(_ => allCompletionsFuture)
  }

  private val futureEmptyList = Future.successful(Nil)
}

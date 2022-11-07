// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, OverflowStrategy}
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.{Identifier, Party, TemplatePartiesFilter}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Streams ACS events (active contracts) in a two step process consisting of:
  * 1) fetching event sequential ids of the active contracts based on the filtering constraints,
  * 2) fetching the active contracts based on the fetched event sequential ids.
  *
  * Details:
  * An input filtering constraint (consisting of parties and template ids) is decomposed into
  * a number of simple filtering constraints (a constraint with one party and at most one template id).
  * For each simple filter in parallel the matching event sequential ids are fetched and then merged into
  * an increasing unique sequence. The elements from this sequence are then batched and each batch is an input
  * to the payload fetching step.
  */
class ACSReader(
    dbDispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    maxNumberOfPayloadsPerPayloadPage: Int,
    maxNumberOfIdsPerIdPage: Int,
    maxNumberOfPagesPerIdPagesBuffer: Int,
    maxWorkingMemoryInBytesForIdPages: Int,
    maxNumberOfParallelIdFetchingQueries: Int,
    maxNumberOfParallelPayloadFetchingQueries: Int,
    globalMaxNumberOfParallelPayloadQueriesLimiter: ConcurrencyLimiter,
    metrics: Metrics,
    executionContext: ExecutionContext,
) {
  import ACSReader._

  private val logger = ContextualizedLogger.get(getClass)

  def streamActiveContractSetEvents(
      filter: TemplatePartiesFilter,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventStorageBackend.Entry[Raw.FlatEvent]], NotUsed] = {
    val allFilterParties = filter.allFilterParties
    val wildcardFilters = filter.wildcardParties.map { party =>
      DecomposedFilter(party, None)
    }
    val decomposedFilters = filter.relation.iterator.flatMap { case (templateId, parties) =>
      parties.iterator.map(party => DecomposedFilter(party, Some(templateId)))
    }.toVector ++ wildcardFilters

    val maxNumberOfParallelIdFetchingQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxNumberOfParallelIdFetchingQueries, executionContext)

    val idQueryPageSizing = IdQueryPageSizing.calculateFrom(
      maxNumberOfIdsPerIdPage = maxNumberOfIdsPerIdPage,
      maxTotalWorkingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages,
      maxNumberOfDecomposedFilters = decomposedFilters.size,
      maxNumberOfPagesPerIdPageBuffer = maxNumberOfPagesPerIdPagesBuffer,
    )

    def buildSourceForFetchingIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      streamIdsFromSeekPagination(
        idQueryPageSizing = idQueryPageSizing,
        pageBufferSize = maxNumberOfPagesPerIdPagesBuffer,
      )(idQuery =>
        maxNumberOfParallelIdFetchingQueriesLimiter.execute {
          dbDispatcher.executeSql(metrics.daml.index.db.getActiveContractIds) { connection =>
            val ids = eventStorageBackend.activeContractEventIds(
              partyFilter = filter.party,
              templateIdFilter = filter.templateId,
              startExclusive = idQuery.fromExclusiveEventSeqId,
              endInclusive = activeAt._2,
              limit = idQuery.numberOfIdsToFetch,
            )(connection)
            logger.debug(
              s"getActiveContractIds $filter returned #${ids.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            ids.toArray
          }
        }
      )

    def fetchPayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] =
      globalMaxNumberOfParallelPayloadQueriesLimiter.execute(
        dbDispatcher.executeSql(metrics.daml.index.db.getActiveContractBatch) { connection =>
          val payloads = queryNonPruned.executeSql(
            eventStorageBackend.activeContractEventBatch(
              eventSequentialIds = ids,
              allFilterParties = allFilterParties,
              endInclusive = activeAt._2,
            )(connection),
            activeAt._1,
            pruned =>
              s"Active contracts request after ${activeAt._1.toHexString} precedes pruned offset ${pruned.toHexString}",
          )(connection, implicitly)
          logger.debug(
            s"getActiveContractBatch returned ${ids.size}/${payloads.size} ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          payloads
        }
      )

    decomposedFilters
      .map(buildSourceForFetchingIds)
      .pipe(mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = maxNumberOfPayloadsPerPayloadPage,
          maxBatchCount = maxNumberOfParallelPayloadFetchingQueries + 1,
        )
      )
      .async
      .addAttributes(
        Attributes.inputBuffer(
          initial = maxNumberOfParallelPayloadFetchingQueries,
          max = maxNumberOfParallelPayloadFetchingQueries,
        )
      )
      .mapAsync(maxNumberOfParallelPayloadFetchingQueries)(fetchPayloads)
  }
}

private[events] object ACSReader {
  private val logger = ContextualizedLogger.get(getClass)

  case class DecomposedFilter(party: Party, templateId: Option[Identifier])

  case class IdQueryParams(fromExclusiveEventSeqId: Long, numberOfIdsToFetch: Int)

  case class IdQueryPageSizing(
      minNumberOfIdsPerPage: Int,
      maxNumberOfIdsPerPage: Int,
  ) {
    assert(minNumberOfIdsPerPage > 0)
    assert(maxNumberOfIdsPerPage >= minNumberOfIdsPerPage)
  }

  object IdQueryPageSizing {
    def calculateFrom(
        maxNumberOfIdsPerIdPage: Int,
        maxTotalWorkingMemoryInBytesForIdPages: Int,
        maxNumberOfDecomposedFilters: Int,
        maxNumberOfPagesPerIdPageBuffer: Int,
    )(implicit loggingContext: LoggingContext): IdQueryPageSizing = {
      // maxIdPageSize can override this if it is smaller
      val smallestIdPageSize = Math.min(10, maxNumberOfIdsPerIdPage)
      // Approximation of how many index entries can be present in a leaf page of a btree index
      // (fetching fewer ids than this only adds round-trip overhead, without decreasing the number of disk page reads per round-trip).
      // Experiments (with default fill ratio for BTree Index) show that:
      // - (party_id, template_id) index has 244 tuples per disk page,
      // - wildcard party_id index has 254 per page.
      // Picking a smaller number accommodates for pruning, deletions, index bloat effect, which all result in a smaller tuples per disk page ratio.
      // maxIdPageSize can override this if it is smaller
      val recommendedMinIdPageSize = Math.min(200, maxNumberOfIdsPerIdPage)
      // An ID takes up 8 bytes
      val maxTotalNumberOfIdsInMemory = maxTotalWorkingMemoryInBytesForIdPages / 8
      // For each filter we have: 1) one page fetched for merge sorting and 2) additional pages residing in the buffer
      val calculatedMaxNumberOfIdPages =
        (maxNumberOfPagesPerIdPageBuffer + 1) * maxNumberOfDecomposedFilters
      val calculatedMaxNumberOfIdsPerPage =
        maxTotalNumberOfIdsInMemory / calculatedMaxNumberOfIdPages
      if (calculatedMaxNumberOfIdsPerPage < smallestIdPageSize) {
        logger.warn(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxNumberOfIdsPerPage] is too low: $smallestIdPageSize is used instead. " +
            s"Warning: API stream memory limits not respected. Warning: Dangerously low maximum ID page size can cause poor streaming performance. " +
            s"Filter size [$maxNumberOfDecomposedFilters] too large?"
        )
        IdQueryPageSizing(smallestIdPageSize, smallestIdPageSize)
      } else if (calculatedMaxNumberOfIdsPerPage < recommendedMinIdPageSize) {
        logger.warn(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxNumberOfIdsPerPage] is very low. " +
            s"Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$maxNumberOfDecomposedFilters] too large?"
        )
        IdQueryPageSizing(calculatedMaxNumberOfIdsPerPage, calculatedMaxNumberOfIdsPerPage)
      } else if (calculatedMaxNumberOfIdsPerPage < maxNumberOfIdsPerIdPage) {
        logger.info(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxNumberOfIdsPerPage] is low. " +
            s"Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$maxNumberOfDecomposedFilters] too large?"
        )
        IdQueryPageSizing(recommendedMinIdPageSize, calculatedMaxNumberOfIdsPerPage)
      } else {
        logger.debug(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxNumberOfIdsPerPage] is high, using [$maxNumberOfIdsPerIdPage] instead."
        )
        IdQueryPageSizing(recommendedMinIdPageSize, maxNumberOfIdsPerIdPage)
      }
    }
  }

  def streamIdsFromSeekPagination(
      idQueryPageSizing: IdQueryPageSizing,
      pageBufferSize: Int,
  )(fetchIdPage: IdQueryParams => Future[Array[Long]]): Source[Long, NotUsed] = {
    assert(pageBufferSize > 0)
    Source
      .unfoldAsync(
        IdQueryParams(
          fromExclusiveEventSeqId = 0L,
          numberOfIdsToFetch = idQueryPageSizing.minNumberOfIdsPerPage,
        )
      ) { params =>
        fetchIdPage(params).map {
          case empty if empty.isEmpty => None
          case nonEmpty =>
            Some(
              IdQueryParams(
                fromExclusiveEventSeqId = nonEmpty.last,
                numberOfIdsToFetch =
                  Math.min(params.numberOfIdsToFetch * 4, idQueryPageSizing.maxNumberOfIdsPerPage),
              ) -> nonEmpty
            )
        }(scala.concurrent.ExecutionContext.parasitic)
      }
      .buffer(pageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity(_))
  }

  @tailrec
  def mergeSort[T: Ordering](sources: Vector[Source[T, NotUsed]]): Source[T, NotUsed] =
    if (sources.isEmpty) Source.empty
    else if (sources.size == 1) sources.head
    else
      mergeSort(
        sources
          .drop(2)
          .appended(
            sources.take(2).reduce(_ mergeSorted _)
          )
      )

  def statefulDeduplicate[T]: () => T => List[T] =
    () => {
      var last = null.asInstanceOf[T]
      elem =>
        if (elem == last) Nil
        else {
          last = elem
          List(elem)
        }
    }
}

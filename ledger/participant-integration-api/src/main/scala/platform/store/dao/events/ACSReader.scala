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

trait ACSReader {
  def acsStream(
      filter: TemplatePartiesFilter,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventStorageBackend.Entry[Raw.FlatEvent]], NotUsed]
}

class FilterTableACSReader(
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    pageSize: Int,
    idPageSize: Int,
    idPageBufferSize: Int,
    idPageWorkingMemoryBytes: Int,
    idFetchingParallelism: Int,
    acsFetchingparallelism: Int,
    metrics: Metrics,
    querylimiter: ConcurrencyLimiter,
    executionContext: ExecutionContext,
) extends ACSReader {
  import FilterTableACSReader._

  private val logger = ContextualizedLogger.get(this.getClass)

  def acsStream(
      filter: TemplatePartiesFilter,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventStorageBackend.Entry[Raw.FlatEvent]], NotUsed] = {
    val allFilterParties = filter.allFilterParties

    val filters = makeSimpleFilters(filter).toVector

    val idQueryLimiter =
      new QueueBasedConcurrencyLimiter(idFetchingParallelism, executionContext)

    val idQueryConfiguration = IdQueryConfiguration(
      maxIdPageSize = idPageSize,
      idPageWorkingMemoryBytes = idPageWorkingMemoryBytes,
      filterSize = filters.size,
      idPageBufferSize = idPageBufferSize,
    )

    def toIdSource(filter: Filter): Source[Long, NotUsed] =
      idSource(
        idQueryConfiguration = idQueryConfiguration,
        pageBufferSize = idPageBufferSize,
      )(idQuery =>
        idQueryLimiter.execute {
          dispatcher.executeSql(metrics.daml.index.db.getActiveContractIds) { connection =>
            val result =
              eventStorageBackend.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
                stakeholder = filter.party,
                templateIdO = filter.templateId,
                startExclusive = idQuery.fromExclusiveEventSeqId,
                endInclusive = activeAt._2,
                limit = idQuery.pageSize,
              )(connection)
            logger.debug(
              s"getActiveContractIds $filter returned #${result.size} ${result.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            result.toArray
          }
        }
      )

    def fetchAcs(ids: Iterable[Long]): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] =
      querylimiter.execute(
        dispatcher.executeSql(metrics.daml.index.db.getActiveContractBatch) { connection =>
          val result = queryNonPruned.executeSql(
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
            s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                .map(last => s"until $last")
                .getOrElse("")}"
          )
          result
        }
      )

    filters
      .map(toIdSource)
      .pipe(mergeSort[Long])
      .statefulMapConcat(statefulDeduplicate)
      .via(
        BatchN(
          maxBatchSize = pageSize,
          maxBatchCount = acsFetchingparallelism + 1,
        )
      )
      .async
      .addAttributes(
        Attributes.inputBuffer(initial = acsFetchingparallelism, max = acsFetchingparallelism)
      )
      .mapAsync(acsFetchingparallelism)(fetchAcs)
  }
}

// TODO etq: Extract utilities common for tx processing to a separate object
object FilterTableACSReader {
  private val logger = ContextualizedLogger.get(this.getClass)

  case class Filter(party: Party, templateId: Option[Identifier])

  def makeSimpleFilters(filter: TemplatePartiesFilter): Seq[Filter] = {
    val wildcardFilters = filter.wildcardParties.map { party =>
      Filter(party, None)
    }
    val filters = filter.relation.iterator.flatMap { case (templateId, parties) =>
      parties.iterator.map(party => Filter(party, Some(templateId)))
    }.toVector ++ wildcardFilters
    filters
  }

  case class IdQuery(fromExclusiveEventSeqId: Long, pageSize: Int)

  case class IdQueryConfiguration(
      minPageSize: Int,
      maxPageSize: Int,
  ) {
    assert(minPageSize > 0)
    assert(maxPageSize >= minPageSize)
  }

  object IdQueryConfiguration {
    def apply(
        maxIdPageSize: Int,
        idPageWorkingMemoryBytes: Int,
        filterSize: Int,
        idPageBufferSize: Int,
    )(implicit loggingContext: LoggingContext): IdQueryConfiguration = {
      val LowestIdPageSize =
        Math.min(10, maxIdPageSize) // maxIdPageSize can override this if it is smaller
      // Approximation how many index entries can be present in one btree index leaf page (fetching smaller than this only adds round-trip overhead, without boiling down to smaller disk read)
      // Experiments show party_id, template_id index has 244 tuples per page, wildcard party_id index has 254 per page (with default fill ratio for BTREE Index)
      // Picking a lower number is for accommodating pruning, deletions, index bloat effect, which boil down to lower tuple per page ratio.
      val RecommendedMinIdPageSize =
        Math.min(200, maxIdPageSize) // maxIdPageSize can override this if it is smaller
      val calculatedMaxIdPageSize =
        idPageWorkingMemoryBytes
          ./(8) // IDs stored in 8 bytes
          ./(
            idPageBufferSize + 1
          ) // for each filter we need one page fetched for merge sorting, and additional pages might reside in the buffer
          ./(filterSize)
      if (calculatedMaxIdPageSize < LowestIdPageSize) {
        logger.warn(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is too low: $LowestIdPageSize is used instead. Warning: API stream memory limits not respected. Warning: Dangerously low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
        )
        IdQueryConfiguration(LowestIdPageSize, LowestIdPageSize)
      } else if (calculatedMaxIdPageSize < RecommendedMinIdPageSize) {
        logger.warn(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is very low. Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
        )
        IdQueryConfiguration(calculatedMaxIdPageSize, calculatedMaxIdPageSize)
      } else if (calculatedMaxIdPageSize < maxIdPageSize) {
        logger.info(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is low. Warning: Low maximum ID page size can cause poor streaming performance. Filter size [$filterSize] too large?"
        )
        IdQueryConfiguration(RecommendedMinIdPageSize, calculatedMaxIdPageSize)
      } else {
        logger.debug(
          s"Calculated maximum ID page size supporting API stream memory limits [$calculatedMaxIdPageSize] is sufficiently high, using [$maxIdPageSize] instead."
        )
        IdQueryConfiguration(RecommendedMinIdPageSize, maxIdPageSize)
      }
    }
  }

  def idSource(
      idQueryConfiguration: IdQueryConfiguration,
      pageBufferSize: Int,
  )(getPage: IdQuery => Future[Array[Long]]): Source[Long, NotUsed] = {
    assert(pageBufferSize > 0)
    Source
      .unfoldAsync(
        IdQuery(0L, idQueryConfiguration.minPageSize)
      ) { query =>
        getPage(query).map {
          case empty if empty.isEmpty => None
          case nonEmpty =>
            Some(
              IdQuery(
                nonEmpty.last,
                Math.min(query.pageSize * 4, idQueryConfiguration.maxPageSize),
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

// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.OverflowStrategy
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.{FilterRelation, Identifier, Party}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.utils.{ConcurrencyLimiter, QueueBasedConcurrencyLimiter}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

trait ACSReader {
  def acsStream(
      filter: FilterRelation,
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
    idFetchingParallelism: Int,
    acsFetchingparallelism: Int,
    metrics: Metrics,
    querylimiter: ConcurrencyLimiter,
    executionContext: ExecutionContext,
) extends ACSReader {
  import FilterTableACSReader._

  private val logger = ContextualizedLogger.get(this.getClass)

  def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventStorageBackend.Entry[Raw.FlatEvent]], NotUsed] = {
    val allFilterParties = filter.keySet
    val filters = filter.iterator.flatMap {
      case (party, templateIds) if templateIds.isEmpty => Iterator(Filter(party, None))
      case (party, templateIds) =>
        templateIds.iterator.map(templateId => Filter(party, Some(templateId)))
    }.toVector

    val idQueryLimiter =
      new QueueBasedConcurrencyLimiter(idFetchingParallelism, executionContext)

    def toIdSource(filter: Filter): Source[Long, NotUsed] =
      idSource(idPageBufferSize)(fromExclusive =>
        idQueryLimiter.execute(
          dispatcher.executeSql(metrics.daml.index.db.getActiveContractIds) { connection =>
            val result = eventStorageBackend.activeContractEventIds(
              partyFilter = filter.party,
              templateIdFilter = filter.templateId,
              startExclusive = fromExclusive,
              endInclusive = activeAt._2,
              limit = idPageSize,
            )(connection)
            logger.debug(
              s"getActiveContractIds $filter returned #${result.size} ${result.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            result
          }
        )
      )

    def fetchAcs(ids: Vector[Long]): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] =
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
      .grouped(pageSize)
      .map(_.toVector)
      .async
      .mapAsync(acsFetchingparallelism)(fetchAcs)
  }
}

private[events] object FilterTableACSReader {
  case class Filter(party: Party, templateId: Option[Identifier])

  def idSource(
      pageBufferSize: Int
  )(getPage: Long => Future[Vector[Long]]): Source[Long, NotUsed] = {
    assert(pageBufferSize > 0)
    Source
      .unfoldAsync(0L) { fromExclusive =>
        getPage(fromExclusive).map {
          case empty if empty.isEmpty => None
          case nonEmpty =>
            Some(
              nonEmpty.last -> nonEmpty
            )
        }(scala.concurrent.ExecutionContext.parasitic)
      }
      .buffer(pageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
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

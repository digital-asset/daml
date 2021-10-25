// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.{Materializer, QueueOfferResult}
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.StorageBackend
import com.daml.platform.store.cache.StringInterning

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Future

class ACSReader(
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    storageBackend: StorageBackend[_], // TODO ACS shrink
    pageSize: Int,
    parallelism: Int,
    metrics: Metrics,
    stringInterning: StringInterning,
    materializer: Materializer,
) {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val idPageSize =
    pageSize * 20 // TODO ACS maybe configure separately? could be *100 as well in theory...

  def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventsTable.Entry[Raw.FlatEvent]], NotUsed] = {
    val allFilterParties = filter.keySet
    ACSReader
      .pullWorkerSource[ACSReader.Query, Vector[EventsTable.Entry[Raw.FlatEvent]]](
        workerParallelism = parallelism,
        materializer = materializer,
      )(
        work = query => {
          logger.debug(s"getActiveContracts query($query)") // TODO ACS more meaningful logging here
          dispatcher
            .executeSql(metrics.daml.index.db.getActiveContracts)(connection =>
              queryNonPruned.executeSql( // TODO ACS do we need this?
                storageBackend.activeContractEvents2(
                  allFilterParties = allFilterParties,
                  partyFilter = query.filter.party,
                  templateIdFilter = query.filter.templateId,
                  excludeParties = query.filter.notParty,
                  startExclusive = query.fromExclusiveEventSeqId,
                  endInclusive = activeAt._2,
                  limit = pageSize,
                  stringInterning = stringInterning,
                )(connection),
                activeAt._1,
                pruned =>
                  s"Active contracts request after ${activeAt._1.toHexString} precedes pruned offset ${pruned.toHexString}",
              )(connection)
            )
            .map { result =>
              // TODO ACS remove
              println(
                s"ACS QUERY: $query to $activeAt results: ${result.map(_.eventSequentialId).mkString("\n")}"
              )
              val newTasks =
                if (result.size < pageSize) Nil
                else query.copy(fromExclusiveEventSeqId = result.last.eventSequentialId) :: Nil
              result -> newTasks
            }(materializer.executionContext)
        },
        initialTasks = ACSReader.buildDisjointFiltersFrom(filter).map(ACSReader.Query(0L, _)),
      )
      .map(_._2)
  }

  def acsStream2Phase(
      filter: FilterRelation,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventsTable.Entry[Raw.FlatEvent]], NotUsed] = {
    val allFilterParties = filter.keySet
    val filters = ACSReader.buildDisjointFiltersFrom(
      filter
    ) // TODO ACS would need another one without the exclusions
    ACSReader
      .pullWorkerSource[ACSReader.Query, Vector[Long]](
        workerParallelism =
          parallelism, // TODO ACS I would keep this parallelism high so we can populate the initial set smooth
        materializer = materializer,
      )(
        work = query => {
          logger.debug(
            s"getActiveContractIds query($query)"
          ) // TODO ACS more meaningful logging here
          dispatcher
            .executeSql(metrics.daml.index.db.getActiveContracts)(
              storageBackend.activeContractEventIds(
                partyFilter = query.filter.party,
                templateIdFilter = query.filter.templateId,
                startExclusive = query.fromExclusiveEventSeqId,
                endInclusive = activeAt._2,
                limit = idPageSize,
                stringInterning = stringInterning,
              )
            )
            .map { result =>
              // TODO ACS remove
              println(
                s"ACS ID QUERY: $query to $activeAt results: $result"
              )
              val newTasks =
                if (result.size < idPageSize) Nil
                else query.copy(fromExclusiveEventSeqId = result.last) :: Nil
              result -> newTasks
            }(materializer.executionContext)
        },
        initialTasks = filters.map(ACSReader.Query(0L, _)),
      )
      .statefulMapConcat {
        val missing: mutable.Set[ACSReader.Filter] = mutable.Set.from(filters)
        val iteratorQueue: mutable.PriorityQueue[(Long, Iterator[Long], ACSReader.Filter)] =
          new mutable.PriorityQueue()(
            Ordering.by[(Long, Iterator[Long], ACSReader.Filter), Long](_._1).reverse
          )
        val additionally: mutable.Map[ACSReader.Filter, Vector[Vector[Long]]] = mutable.Map.empty
        var lastElem: Long = -1
        var lastResult: List[Long] = Nil
        var results: List[List[Long]] = Nil
        def refill(filter: ACSReader.Filter) =
          additionally.get(filter) match {
            case Some(ids) if ids.head.isEmpty => // finished
              additionally -= filter
              missing -= filter

            case Some(ids) =>
              if (ids.size == 1) additionally -= filter
              else additionally += (filter -> ids.tail)
              missing -= filter
              iteratorQueue.enqueue((ids.head.head, ids.head.iterator.drop(1), filter))

            case None =>
              missing += filter
          }
        @tailrec
        def takeMergedIdStream: List[List[Long]] =
          if (missing.nonEmpty) { // one of the queues went dry, which needs more data
            val r = results.reverse
            results = Nil
            r
          } else if (iteratorQueue.isEmpty) { // finished, we flush the internal data
            val r = (lastResult.reverse :: results).reverse
            results = Nil
            r
          } else { // fetch one element from the priorityQueue
            val newElem = iteratorQueue.dequeue() match {
              case (headId, tailIdIterator, filter) if tailIdIterator.hasNext =>
                iteratorQueue.enqueue((tailIdIterator.next(), tailIdIterator, filter))
                headId
              case (headId, _, filter) =>
                refill(filter)
                headId
            }
            if (lastElem != newElem) {
              if (lastResult.size == pageSize) { // TODO ACS ouch, need to figure something not O(N) here (arrays with pointers, or some array builder would work as well)
                results = lastResult.reverse :: results
                lastResult = Nil
              }
              lastResult = newElem :: lastResult
              lastElem = newElem
            }
            takeMergedIdStream
          }
        () => { case (query, ids) =>
          // place new results
          additionally += (query.filter -> additionally
            .getOrElse(query.filter, Vector.empty)
            .:+(ids))
          if (ids.size < idPageSize && ids.nonEmpty) {
            // add an additional empty to the end of the additional to mark completion
            additionally += (query.filter -> additionally
              .getOrElse(query.filter, Vector.empty)
              .:+(Vector.empty))
          }
          if (missing(query.filter)) refill(query.filter)
          val r = takeMergedIdStream
          println(s"emitting: $r")
          r
        }
      }
      .async
      .mapAsync(parallelism) { ids =>
        logger.debug(s"getActiveContracts") // TODO ACS more meaningful logging here
        dispatcher
          .executeSql(metrics.daml.index.db.getActiveContracts)(connection =>
            queryNonPruned.executeSql( // TODO ACS do we need this?
              {
                val r = storageBackend.activeContractEvents3(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAt._2,
                  stringInterning = stringInterning,
                )(connection)
                println(s"ACS QUERY ${r.map(_.eventSequentialId)}")
                r
              },
              activeAt._1,
              pruned =>
                s"Active contracts request after ${activeAt._1.toHexString} precedes pruned offset ${pruned.toHexString}",
            )(connection)
          )
      }
  }

}

object ACSReader {

  def pullWorkerSource[TASK: Ordering, RESULT](
      workerParallelism: Int,
      // TODO ACS globalWorkerParallelims: ???,
      materializer: Materializer,
  )(
      work: TASK => Future[(RESULT, Iterable[TASK])],
      initialTasks: Iterable[TASK],
  ): Source[(TASK, RESULT), NotUsed] = {
    val priorityQueue =
      new scala.collection.mutable.PriorityQueue[TASK]()(implicitly[Ordering[TASK]].reverse)
    var runningTasks: Int = 0
    val (signalQueue, signalSource) = Source
      .queue[Unit](128) // TODO ACS maybe make it based on the initial input?
      .preMaterialize()(materializer)
    def addTask(task: TASK): Unit = {
      priorityQueue.enqueue(task)
      signalQueue.offer(()) match {
        case QueueOfferResult.Enqueued => ()
        case QueueOfferResult.Dropped =>
          throw new Exception("Cannot enqueue signal: dropped. Queue bufferSize not big enough?")
        case QueueOfferResult.Failure(_) => () // stream already failed
        case QueueOfferResult.QueueClosed => () // stream already closed
      }
    }

    initialTasks.foreach(addTask)
    signalSource
      .mapAsyncUnordered(workerParallelism) { _ =>
        val task = priorityQueue.synchronized {
          runningTasks += 1
          priorityQueue.dequeue()
        }
        work(task).map(results =>
          priorityQueue.synchronized(results match {
            case (result, newTasks)
                if newTasks.isEmpty && priorityQueue.isEmpty && runningTasks == 1 =>
              signalQueue.complete()
              task -> result

            case (result, newTasks) =>
              runningTasks -= 1
              newTasks.foreach(addTask)
              task -> result
          })
        )(materializer.executionContext)
      }
  }

  case class Filter(party: Party, templateId: Option[Ref.Identifier], notParty: Set[Party])

  case class Query(fromExclusiveEventSeqId: Long, filter: Filter)

  object Query {
    implicit val ordering: Ordering[Query] = Ordering.by[Query, Long](_.fromExclusiveEventSeqId)
  }

  def buildDisjointFiltersFrom(filterRelation: FilterRelation): Iterable[Filter] = {
    val (allWildCardParties, wildCardPartyFilters) = filterRelation.iterator
      .collect {
        case (party, templateIds) if templateIds.isEmpty => party
      }
      .foldLeft(Set.empty[Ref.Party] -> List.empty[Filter]) {
        case ((seenParties, filtersAcc), party) =>
          (
            seenParties + party,
            Filter(party, None, seenParties) :: filtersAcc,
          )
      }
    val r = filterRelation.iterator
      .filter(_._2.nonEmpty)
      .foldLeft(Map.empty[Ref.Identifier, Set[Ref.Party]] -> wildCardPartyFilters) {
        case ((seenTemplateIds, filtersAcc), (party, templates)) =>
          (
            seenTemplateIds ++ templates.iterator
              .map(templateId =>
                (
                  templateId,
                  seenTemplateIds.getOrElse(templateId, Set.empty) + party,
                )
              ),
            templates
              .foldLeft(filtersAcc) { case (acc, templateId) =>
                Filter(
                  party,
                  Some(templateId),
                  allWildCardParties ++ seenTemplateIds.getOrElse(templateId, Set.empty),
                ) :: acc
              },
          )
      }
      ._2
      .reverse
    println(s"Disjoint filters: $r") // TODO ACS remove
    r
  }
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.definitions.LedgerApiErrors.ParticipantBackpressure
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.EventStorageBackend

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait ACSReader {
  def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventsTable.Entry[Raw.FlatEvent]], NotUsed]
}

class FilterTableACSReader(
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    pageSize: Int,
    idPageSize: Int,
    idFetchingParallelism: Int,
    acsFetchingparallelism: Int,
    metrics: Metrics,
    materializer: Materializer,
) extends ACSReader {
  import FilterTableACSReader._

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[Vector[EventsTable.Entry[Raw.FlatEvent]], NotUsed] = {
    implicit val errorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)

    val allFilterParties = filter.keySet
    val tasks = filter.iterator
      .flatMap {
        case (party, templateIds) if templateIds.isEmpty => Iterator(Filter(party, None))
        case (party, templateIds) =>
          templateIds.iterator.map(templateId => Filter(party, Some(templateId)))
      }
      .map(QueryTask(0L, _))
      .toVector

    pullWorkerSource[QueryTask, Vector[Long]](
      workerParallelism = idFetchingParallelism,
      materializer = materializer,
    )(
      query =>
        dispatcher
          .executeSql(metrics.daml.index.db.getActiveContractIds)(
            eventStorageBackend.activeContractEventIds(
              partyFilter = query.filter.party,
              templateIdFilter = query.filter.templateId,
              startExclusive = query.fromExclusiveEventSeqId,
              endInclusive = activeAt._2,
              limit = idPageSize,
            )
          )
          .map { result =>
            val newTasks =
              if (result.size < idPageSize) None
              else Some(query.copy(fromExclusiveEventSeqId = result.last))
            logger.debug(s"getActiveContractIds $query returned #${result.size} ${result.lastOption
              .map(last => s"until $last")
              .getOrElse("")}")
            result -> newTasks
          }(materializer.executionContext),
      initialTasks = tasks,
    )
      .map({ case (queryTask, results) => queryTask.filter -> results })
      .statefulMapConcat(
        mergeIdStreams(
          tasks = tasks.map(_.filter),
          outputBatchSize = pageSize,
          inputBatchSize = idPageSize,
          metrics = metrics,
        )
      )
      .async
      .mapAsync(acsFetchingparallelism) { ids =>
        dispatcher
          .executeSql(metrics.daml.index.db.getActiveContractBatch) { connection =>
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
      }
  }

}

private[events] object FilterTableACSReader {
  private val logger = ContextualizedLogger.get(this.getClass)

  case class Filter(party: Party, templateId: Option[Ref.Identifier])

  case class QueryTask(fromExclusiveEventSeqId: Long, filter: Filter)

  object QueryTask {
    implicit val ordering: Ordering[QueryTask] =
      Ordering.by[QueryTask, Long](_.fromExclusiveEventSeqId)
  }

  /** This Source implementation solves the following problem:
    *  - let us have n TASKs, which are ordered
    *  - let us define some work over these tasks, which gives us a RESULT and a possible continuation of the TASK
    *  - let us have configurable parallelism to work on these TASKs
    *  This implementation ensures that all the time the smallest available TASK will be picked for work.
    *
    * Please note:
    *  - If workerParallelism is one, this should result in monotonously increasing execution sequence
    *    (regardless of the demand downstream)
    *  - If workerParallelism is equal or bigger than the number of initial n tasks, and downstream is faster,
    *    then prioritization has no time to kick in (the backing priority queue will have mostly one element), so
    *    execution order will be similar to simple parallel execution of sequences of tasks
    *
    * @param workerParallelism defines the maximum parallelism of unordered processing.
    *                          Naturally capped by size of initialTasks
    * @param work The worker function, asynchronous computation should return a RESULT,
    *             and the next TASK, or no TASK if TASK processing is finished
    * @param initialTasks The collection of initial TASKS for execution
    * @tparam TASK type of TASKs, needs to have an Ordering defined.
    *              Always the smallest task will be selected for execution
    * @tparam RESULT The type of the RESULT
    * @return A Source, with TASK, RESULT pairs.
    *         The ordering of the elements will simply follow the work completion order.
    *         Completes, if all TASKS finish (for all of them a final work executed, giving no continuation)
    *         Fails if work processing fails.
    */
  def pullWorkerSource[TASK: Ordering, RESULT](
      workerParallelism: Int,
      materializer: Materializer,
  )(
      work: TASK => Future[(RESULT, Option[TASK])],
      initialTasks: Iterable[TASK],
  )(implicit errorLogger: ContextualizedErrorLogger): Source[(TASK, RESULT), NotUsed] = if (
    initialTasks.isEmpty
  ) Source.empty
  else {
    val (signalQueue, signalSource) = Source
      .queue[Unit](initialTasks.size)
      .preMaterialize()(materializer)

    val queueState = new QueueState(signalQueue, initialTasks)

    signalSource
      .mapAsyncUnordered(workerParallelism) { _ =>
        val task = queueState.startTask()
        work(task).map { case (result, nextTask) =>
          queueState.finishTask(nextTask)
          task -> result
        }(ExecutionContext.parasitic)
      }
  }

  /** Helper class to capture stateful  operations of pullWorkerSource
    */
  class QueueState[TASK: Ordering](
      signalQueue: BoundedSourceQueue[Unit],
      initialTasks: Iterable[TASK],
  )(implicit errorLogger: ContextualizedErrorLogger) {
    private val priorityQueue =
      new mutable.PriorityQueue[TASK]()(implicitly[Ordering[TASK]].reverse)
    private var runningTasks: Int = 0

    initialTasks.foreach(addTask)

    def startTask(): TASK = synchronized {
      runningTasks += 1
      priorityQueue.dequeue()
    }

    def finishTask(nextTask: Option[TASK]): Unit = synchronized {
      nextTask match {
        case None if priorityQueue.isEmpty && runningTasks == 1 =>
          signalQueue.complete()

        case newTask =>
          runningTasks -= 1
          newTask.foreach(addTask)
      }
    }

    private def addTask(task: TASK): Unit = {
      priorityQueue.enqueue(task)
      signalQueue.offer(()) match {
        case QueueOfferResult.Enqueued => ()
        case QueueOfferResult.Dropped =>
          throw ParticipantBackpressure
            .Rejection(
              "Cannot enqueue signal: dropped. ACS reader queue bufferSize not big enough."
            )
            .asGrpcError
        case QueueOfferResult.Failure(f) =>
          throw LedgerApiErrors.InternalError
            .Buffer("Failed to enqueue in ACS reader queue state: Internal failure", Some(f))
            .asGrpcError
        case QueueOfferResult.QueueClosed =>
          throw LedgerApiErrors.InternalError
            .Buffer("Failed to enqueue in ACS reader queue state: Queue closed", None)
            .asGrpcError
      }
    }
  }

  /** A stateful merge function to be used in akka-streams statefulMapConcat.
    * This merge function receives a stream of id-ranges where each range is associated with a "task",
    * and creates an evenly batched stream of ordered and deduplicated ids.
    *
    * @param tasks The initial set of tasks. This function expects for each task to receive:
    *              - the input id ranges must be monotonically increasing
    *                 (both in one batch, and all batches related to the same task)
    *              - the size of all input id ranges except the last one must be exactly `inputBatchSize`
    *              - the size of the last input id range must not be equal to `inputBatchSize` (but may be empty)
    * @param outputBatchSize The output stream will contain batches with this size, except the last one
    * @param inputBatchSize Batch size of the input id ranges
    */
  def mergeIdStreams[TASK](
      tasks: Iterable[TASK],
      outputBatchSize: Int,
      inputBatchSize: Int,
      metrics: Metrics,
  )(implicit
      loggingContext: LoggingContext
  ): () => ((TASK, Iterable[Long])) => Vector[Vector[Long]] = () => {
    val outputQueue = new BatchedDistinctOutputQueue(outputBatchSize)
    val taskQueue = new MergingTaskQueue[TASK](outputQueue.push)
    val taskTracker = new TaskTracker[TASK](tasks, inputBatchSize)

    { case (task, ids) =>
      @tailrec def go(next: (Option[(Iterable[Long], TASK)], Boolean)): Unit = {
        next._1.foreach(taskQueue.push)
        if (next._2) taskQueue.runUntilATaskEmpty match {
          case Some(task) => go(taskTracker.finished(task))
          case None => outputQueue.flushPartialBatch()
        }
        else ()
      }
      Timed.value(
        metrics.daml.index.acsRetrievalSequentialProcessing, {
          go(taskTracker.add(task, ids))
          val result = outputQueue.flushOutput
          logger.debug(
            s"acsRetrievalSequentialProcessing received $task with #{${ids.size}} ${ids.lastOption
              .map(last => s"until $last ")
              .getOrElse("")}and produced ${result.size}"
          )
          result
        },
      )
    }
  }

  /** Helper class to encapsulate stateful output batching, and deduplication.
    */
  class BatchedDistinctOutputQueue(batchSize: Int) {
    private var last: Long = -1
    private var buff: Array[Long] = Array.ofDim(batchSize)
    private var buffIndex: Int = 0
    private var output: Vector[Vector[Long]] = Vector.empty

    /** Add one Long entry to the output.
      */
    def push(l: Long): Unit = {
      if (last != l) {
        buff.update(buffIndex, l)
        buffIndex += 1
        last = l
      }
      if (buffIndex == batchSize) {
        output = output :+ buff.toVector
        buff = Array.ofDim(batchSize)
        buffIndex = 0
      }
    }

    /** @return all the currently available buffered output
      */
    def flushOutput: Vector[Vector[Long]] = {
      val result = output
      output = Vector.empty
      result
    }

    /** Calling this function adds the current partially filled batch to the output returned by flushOutput.
      * This need to be called in order to retrieve all results if processing finishes
      * (normally only evenly sized batches emitted, and partial results will be buffered).
      */
    def flushPartialBatch(): Unit =
      if (buffIndex != 0) {
        output = output :+ buff.view.take(buffIndex).toVector
        buff = Array.ofDim(batchSize)
        buffIndex = 0
      }
  }

  /** Helper class to encapsulate stateful merging of multiple ordered streams.
    */
  class MergingTaskQueue[TASK](output: Long => Unit) {
    private val iteratorQueue: mutable.PriorityQueue[(Long, Iterator[Long], TASK)] =
      new mutable.PriorityQueue()(
        Ordering.by[(Long, Iterator[Long], TASK), Long](_._1).reverse
      )

    /** Adding a new task to the queue
      * @param task the id sequence received from upstream and the task identifier.
      */
    def push(task: (Iterable[Long], TASK)): Unit = {
      val iterator = task._1.iterator
      if (iterator.hasNext)
        iteratorQueue.enqueue((iterator.next(), iterator, task._2))
    }

    /** Consume all task's iterators until the first one completes.
      * This populates the merged sequence to the output callback function.
      *
      * @return Some TASK in case an iterator finished for one, or None in case the whole processing is finished.
      */
    def runUntilATaskEmpty: Option[TASK] = {
      @tailrec def go(): Option[TASK] = if (iteratorQueue.isEmpty) None
      else {
        val (elem, iterator, task) = iteratorQueue.dequeue()
        output(elem)
        if (iterator.hasNext) {
          iteratorQueue.enqueue((iterator.next(), iterator, task))
          go()
        } else {
          Some(task)
        }
      }
      go()
    }
  }

  /** Helper class to encapsulate stateful tracking of task streams.
    */
  class TaskTracker[TASK](allTasks: Iterable[TASK], inputBatchSize: Int) {
    private val idle: mutable.Set[TASK] = mutable.Set.empty
    private val queuedRanges: mutable.Map[TASK, Vector[Iterable[Long]]] = mutable.Map.empty

    idle ++= allTasks

    /** Add one entry from upstream
      * @param task the TASK identifier
      * @param ids the ordered sequence of ids
      * @return An optional entry to be added to the MergingTaskQueue, and a flag if further merging needed
      */
    def add(task: TASK, ids: Iterable[Long]): (Option[(Iterable[Long], TASK)], Boolean) = {
      val toEnqueue =
        if (idle(task)) queueEntry(task, ids)
        else {
          queuedRanges += (task -> queuedRanges.getOrElse(task, Vector.empty).:+(ids))
          None
        }
      if (
        ids.nonEmpty && ids.size < inputBatchSize
      ) // add one more empty signalling the end of one task-stream
        queuedRanges += (task -> queuedRanges.getOrElse(task, Vector.empty).:+(Vector.empty))
      idle -= task
      (toEnqueue, idle.isEmpty)
    }

    /** If merging finished with a TASK running out of elements, this method will populate a suitable continuation,
      * if applicable.
      * @return An optional entry to be added to the MergingTaskQueue, and a flag if further merging needed
      */
    def finished(task: TASK): (Option[(Iterable[Long], TASK)], Boolean) =
      queuedRanges.get(task) match {
        case Some(idsQueue) =>
          val newIdsQueue = idsQueue.drop(1)
          if (newIdsQueue.isEmpty) queuedRanges.remove(task)
          else queuedRanges += (task -> newIdsQueue)
          (queueEntry(task, idsQueue.head), true)
        case None =>
          idle.add(task)
          (None, false)
      }

    private def queueEntry(task: TASK, ids: Iterable[Long]): Option[(Iterable[Long], TASK)] =
      if (ids.isEmpty) None
      else {
        Some((ids, task))
      }

  }
}

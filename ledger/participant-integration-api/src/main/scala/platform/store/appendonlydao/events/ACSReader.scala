// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import akka.stream.scaladsl.Source
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.collection.mutable
import scala.concurrent.Future

trait ACSReader {
  def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed]
}

class FilterTableACSReader extends ACSReader {

  override def acsStream(
      filter: FilterRelation,
      activeAt: (Offset, Long),
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] =
    throw new NotImplementedError() // FIXME will be implemented in a later increment

}

private[events] object FilterTableACSReader {

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
  ): Source[(TASK, RESULT), NotUsed] = if (initialTasks.isEmpty) Source.empty
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
        }(DirectExecutionContext)
      }
  }

  /** Helper class to capture stateful  operations of pullWorkerSource
    */
  class QueueState[TASK: Ordering](
      signalQueue: BoundedSourceQueue[Unit],
      initialTasks: Iterable[TASK],
  ) {
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
          throw new Exception("Cannot enqueue signal: dropped. Queue bufferSize not big enough.")
        case QueueOfferResult.Failure(_) => () // stream already failed
        case QueueOfferResult.QueueClosed => () // stream already closed
      }
    }
  }
}

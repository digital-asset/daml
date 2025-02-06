// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.concurrent.{ExecutionContext, Promise, blocking}

/** The [[SynchronizerOutboxQueue]] connects a [[SynchronizerTopologyManager]] and a `SynchronizerOutbox`.
  * The topology manager enqueues transactions that the synchronizer outbox will pick up and send
  * to the synchronizer to be sequenced and distributed to the nodes in the synchronizer.
  *
  * On the one hand, [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#enqueue]] may be called at any point to add
  * more topology transactions to the queue. On the other hand, each invocation of
  * [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#dequeue]] must be followed by either
  * [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#requeue]] or
  * [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#completeCycle]], before
  * [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#dequeue]] is called again.
  */
class SynchronizerOutboxQueue(
    val loggerFactory: NamedLoggerFactory
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val unsentQueue =
    new scala.collection.mutable.Queue[(Traced[GenericSignedTopologyTransaction], Promise[Unit])]
  private val inProcessQueue =
    new scala.collection.mutable.Queue[(Traced[GenericSignedTopologyTransaction], Promise[Unit])]

  /** To be called by the topology manager whenever new topology transactions have been validated.
    */
  def enqueue(
      txs: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): AsyncResult[Unit] = blocking(synchronized {
    logger.debug(s"enqueuing: $txs")
    val p = Promise[Unit]()
    unsentQueue.enqueueAll(txs.map(Traced(_) -> p)).discard
    AsyncResult(FutureUnlessShutdown.outcomeF(p.future))
  })

  def numUnsentTransactions: Int = blocking(synchronized(unsentQueue.size))
  def numInProcessTransactions: Int = blocking(synchronized(inProcessQueue.size))

  /** Marks up to `limit` transactions as pending and returns those transactions.
    * @param limit batch size
    * @return the topology transactions that have been marked as pending.
    */
  def dequeue(limit: PositiveInt)(implicit
      traceContext: TraceContext
  ): Seq[GenericSignedTopologyTransaction] = blocking(synchronized {
    val txs = unsentQueue.take(limit.value).toList
    logger.debug(s"dequeuing: $txs")
    require(
      inProcessQueue.isEmpty,
      s"tried to dequeue while pending wasn't empty: ${inProcessQueue.toSeq}",
    )
    inProcessQueue.enqueueAll(txs)
    unsentQueue.dropInPlace(limit.value)
    inProcessQueue.toSeq.map { case (Traced(tx), _) => tx }
  })

  /** Marks the currently pending transactions as unsent and adds them to the front of the queue in the same order.
    */
  def requeue()(implicit traceContext: TraceContext): Unit = blocking(synchronized {
    logger.debug(s"requeuing $inProcessQueue")
    unsentQueue.prependAll(inProcessQueue)
    inProcessQueue.clear()
  })

  /** Clears the currently pending transactions.
    */
  def completeCycle(observed: Boolean)(implicit traceContext: TraceContext): Unit = blocking(
    synchronized {
      inProcessQueue.foreach { case (_, promise) =>
        promise
          .tryComplete(
            Either
              .cond(
                observed,
                (),
                TopologyManagerError.TimeoutWaitingForTransaction.Failure().asGrpcError,
              )
              .toTry
          )
          .discard
      }
      logger.debug(s"completeCycle $inProcessQueue")
      inProcessQueue.clear()
    }
  )

}

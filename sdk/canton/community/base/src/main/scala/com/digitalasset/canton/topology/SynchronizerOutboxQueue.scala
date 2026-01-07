// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.show.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Mutex

import scala.concurrent.{ExecutionContext, Promise}

/** The [[SynchronizerOutboxQueue]] connects a [[SynchronizerTopologyManager]] and a
  * `SynchronizerOutbox`. The topology manager enqueues transactions that the synchronizer outbox
  * will pick up and send to the synchronizer to be sequenced and distributed to the nodes in the
  * synchronizer.
  *
  * On the one hand, [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#enqueue]] may be
  * called at any point to add more topology transactions to the queue. On the other hand, each
  * invocation of [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#dequeue]] must be
  * followed by either [[com.digitalasset.canton.topology.SynchronizerOutboxQueue#requeue]] or
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
  private val lock = new Mutex()

  /** To be called by the topology manager whenever new topology transactions have been validated.
    */
  def enqueue(
      txs: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): AsyncResult[Unit] = {
    logger.debug(s"enqueuing: ${txs.map(_.hash)}")
    (lock.exclusive {
      val p = Promise[Unit]()
      unsentQueue.enqueueAll(txs.map(Traced(_) -> p)).discard
      AsyncResult(FutureUnlessShutdown.outcomeF(p.future))
    })
  }

  def numUnsentTransactions: Int = (lock.exclusive(unsentQueue.size))
  def numInProcessTransactions: Int = (lock.exclusive(inProcessQueue.size))

  /** Marks up to `limit` transactions as pending and returns those transactions.
    * @param limit
    *   batch size
    * @return
    *   the topology transactions that have been marked as pending.
    */
  def dequeue(limit: PositiveInt)(implicit
      traceContext: TraceContext
  ): Seq[GenericSignedTopologyTransaction] = {
    val (txHashes, ret) = (lock.exclusive {
      val txs = unsentQueue.take(limit.value).toList
      // using the show interpolator to force the pretty instance for Traced instead of the normal toString
      val txHashes = tracedHashes(txs)
      require(
        inProcessQueue.isEmpty,
        s"tried to dequeue while pending wasn't empty: ${inProcessQueue.toSeq}",
      )
      inProcessQueue.enqueueAll(txs)
      unsentQueue.dropInPlace(limit.value)
      val ret = inProcessQueue.toSeq.map { case (Traced(tx), _) => tx }
      (txHashes, ret)
    })
    // using the show interpolator to force the pretty instance for Traced instead of the normal toString
    logger.debug(show"dequeuing: $txHashes")
    ret
  }

  /** Marks the currently pending transactions as unsent and adds them to the front of the queue in
    * the same order.
    */
  def requeue()(implicit traceContext: TraceContext): Unit = {
    val tmpHashes = (lock.exclusive {
      // using the show interpolator to force the pretty instance for Traced instead of the normal toString
      val tmpHashes = tracedHashes(inProcessQueue)
      unsentQueue.prependAll(inProcessQueue)
      inProcessQueue.clear()
      tmpHashes
    })
    logger.debug(show"Requeued $tmpHashes")
  }

  /** Clears the currently pending transactions.
    */
  def completeCycle()(implicit traceContext: TraceContext): Unit = {
    val txHashes = (
      lock.exclusive {
        inProcessQueue.foreach { case (_, promise) =>
          promise.trySuccess(()).discard
        }
        val txHashes = tracedHashes(inProcessQueue)
        // using the show interpolator to force the pretty instance for Traced instead of the normal toString
        inProcessQueue.clear()
        txHashes
      }
    )
    logger.debug(show"completeCycle $txHashes")
  }

  private def tracedHashes(
      txs: Iterable[(Traced[GenericSignedTopologyTransaction], Promise[Unit])]
  ) =
    txs.map(_._1.map(_.hash)).toSeq

}

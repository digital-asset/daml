// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.concurrent.blocking

/** The [[DomainOutboxQueue]] connects a [[DomainTopologyManager]] and a `DomainOutbox`.
  * The topology manager enqueues transactions that the domain outbox will pick up and send
  * to the domain to be sequenced and distributed to the nodes in the domain.
  *
  * On the one hand, [[com.digitalasset.canton.topology.DomainOutboxQueue#enqueue]] may be called at any point to add
  * more topology transactions to the queue. On the other hand, each invocation of
  * [[com.digitalasset.canton.topology.DomainOutboxQueue#dequeue]] must be followed by either
  * [[com.digitalasset.canton.topology.DomainOutboxQueue#requeue]] or
  * [[com.digitalasset.canton.topology.DomainOutboxQueue#completeCycle]], before
  * [[com.digitalasset.canton.topology.DomainOutboxQueue#dequeue]] is called again.
  */
class DomainOutboxQueue(val loggerFactory: NamedLoggerFactory) extends NamedLogging {

  private val unsentQueue =
    new scala.collection.mutable.Queue[Traced[GenericSignedTopologyTransaction]]
  private val inProcessQueue =
    new scala.collection.mutable.Queue[Traced[GenericSignedTopologyTransaction]]

  /** To be called by the topology manager whenever new topology transactions have been validated.
    */
  def enqueue(
      txs: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): Unit = blocking(synchronized {
    logger.debug(s"enqueuing: $txs")
    unsentQueue.enqueueAll(txs.map(Traced(_))).discard
  })

  def numUnsentTransactions: Int = blocking(synchronized(unsentQueue.size))
  def numInProcessTransactions: Int = blocking(synchronized(inProcessQueue.size))

  /** Marks up to `limit` transactions as pending and returns those transactions.
    * @param limit batch size
    * @return the topology transactions that have been marked as pending.
    */
  def dequeue(limit: Int)(implicit
      traceContext: TraceContext
  ): Seq[GenericSignedTopologyTransaction] = blocking(synchronized {
    val txs = unsentQueue.take(limit).toList
    logger.debug(s"dequeuing: $txs")
    require(
      inProcessQueue.isEmpty,
      s"tried to dequeue while pending wasn't empty: ${inProcessQueue.toSeq}",
    )
    inProcessQueue.enqueueAll(txs)
    unsentQueue.dropInPlace(limit)
    inProcessQueue.toSeq.map(_.value)
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
  def completeCycle()(implicit traceContext: TraceContext): Unit = blocking(synchronized {
    logger.debug(s"completeCycle $inProcessQueue")
    inProcessQueue.clear()
  })

}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Mempool,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.utils.Miscellaneous.dequeueN
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.tracing.TraceContext

import MempoolModuleMetrics.{emitRequestStats, emitStateStats}

/** Simple, non-crash-fault-tolerant in-memory mempool implementation.
  *
  * Crash fault-tolerance is not strictly needed because the sequencer client will re-send the
  * requests if they are lost before being ordered.
  */
class MempoolModule[E <: Env[E]](
    config: MempoolModuleConfig,
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    mempoolState: MempoolState = new MempoolState(),
)(implicit mc: MetricsContext)
    extends Mempool[E] {

  private type IngressLabelOutcome = metrics.ingress.labels.outcome.values.OutcomeValue

  override def receiveInternal(message: Mempool.Message)(implicit
      context: E#ActorContextT[Mempool.Message],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(message)

    message match {

      case Mempool.Start =>
        scheduleMempoolBatchCreationClockTick()
      // From clients
      case r @ Mempool.OrderRequest(tracedTx, from, sender) =>
        val orderingRequest = tracedTx.value
        val outcome: IngressLabelOutcome = // Help type inference
          if (mempoolState.receivedOrderRequests.sizeIs == config.maxQueueSize) {
            val rejectionMessage =
              s"mempool received client request but the queue is full (${config.maxQueueSize}), dropping it"
            logger.info(rejectionMessage)
            from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
            metrics.ingress.labels.outcome.values.QueueFull
          } else {
            val payloadSize = orderingRequest.payload.size()
            if (payloadSize > config.maxRequestPayloadBytes) {
              val rejectionMessage =
                s"mempool received client request of size $payloadSize " +
                  s"but it exceeds the maximum (${config.maxRequestPayloadBytes}), dropping it"
              logger.warn(rejectionMessage)
              from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
              metrics.ingress.labels.outcome.values.RequestTooBig
            } else {
              mempoolState.receivedOrderRequests.enqueue(r)
              from.foreach(_.asyncSend(SequencerNode.RequestAccepted))
              if (mempoolState.receivedOrderRequests.sizeIs >= config.minRequestsInBatch.toInt) {
                // every time we receive a new transaction we only try to create new batches if we've reached
                // the configured minimum batch size. alternatively batches are also attempted creation on the configured
                // interval or when explicitly requested by availability
                createAndSendBatches(messageType)
              }
              emitStateStats(metrics, mempoolState)
              metrics.ingress.labels.outcome.values.Success
            }
          }
        emitRequestStats(metrics)(orderingRequest, sender, outcome)

      // From local availability
      case Mempool.CreateLocalBatches(atMost) =>
        logger.debug(
          s"$messageType mempool received batch request from local availability " +
            s"(maxRequestsInBatch: ${config.maxRequestsInBatch})"
        )

        // whenever availability asks for a specific amount of batches,
        // we deliberately forget what's still pending from the last request
        // and we just care about what it is asking this time around.
        mempoolState.toBeProvidedToAvailability = atMost.toInt

        createAndSendBatches(messageType)
        emitStateStats(metrics, mempoolState)

      case Mempool.MempoolBatchCreationClockTick =>
        logger.trace(
          s"Mempool received batch creation clock tick (maxRequestsInBatch: ${config.maxRequestsInBatch})"
        )
        createAndSendBatches(messageType)
        scheduleMempoolBatchCreationClockTick()
    }
  }

  private def scheduleMempoolBatchCreationClockTick()(implicit
      context: E#ActorContextT[Mempool.Message],
      traceContext: TraceContext,
  ): Unit = {
    val interval = config.maxBatchCreationInterval
    logger.trace(s"Scheduling mempool batch creation clock tick in $interval")
    val _ = context.delayedEvent(interval, Mempool.MempoolBatchCreationClockTick)
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def createAndSendBatches(
      messageType: String
  )(implicit context: E#ActorContextT[Mempool.Message]): Unit =
    while (
      mempoolState.receivedOrderRequests.nonEmpty && mempoolState.toBeProvidedToAvailability > 0
    ) {
      mempoolState.toBeProvidedToAvailability -= 1
      createAndSendBatch(messageType)
      emitStateStats(metrics, mempoolState)
    }

  private def createAndSendBatch(
      messageType: String
  )(implicit context: E#ActorContextT[Mempool.Message]): Unit = {
    val requests = dequeueN(mempoolState.receivedOrderRequests, config.maxRequestsInBatch).map(_.tx)
    context.withNewTraceContext { implicit traceContext =>
      logger.debug(
        s"$messageType: mempool sending batch to local availability with the following tids ${requests
            .flatMap(_.traceContext.traceId)}"
      )
      availability.asyncSendTraced(Availability.LocalDissemination.LocalBatchCreated(requests))
    }
    emitStateStats(metrics, mempoolState)
  }
}

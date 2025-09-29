// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Mempool,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.dequeueN
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.opentelemetry.api.trace.{StatusCode, Tracer}

import java.time.Instant

import MempoolModuleMetrics.{emitRequestStats, emitStateStats}

/** Simple, non-crash-fault-tolerant in-memory mempool implementation.
  *
  * Crash fault-tolerance is not strictly needed because the sequencer client will re-send the
  * requests if they are lost before being ordered.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class MempoolModule[E <: Env[E]](
    config: MempoolModuleConfig,
    mempoolState: MempoolState,
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext, tracer: Tracer)
    extends Mempool[E] {

  private type IngressLabelOutcome = metrics.ingress.labels.outcome.values.OutcomeValue

  override def receiveInternal(message: Mempool.Message)(implicit
      context: E#ActorContextT[Mempool.Message],
      traceContext: TraceContext,
  ): Unit = {
    import mempoolState.*

    lazy val messageType = shortType(message)

    message match {

      case Mempool.Start =>
        scheduleMempoolBatchCreationClockTick()
      // From clients
      case r @ Mempool.OrderRequest(tracedTx, from, sender) =>
        val orderingRequest = tracedTx.value
        val span = startSpan("BFTOrderer.Mempool")

        val outcome: IngressLabelOutcome = // Help type inference
          if (!canDisseminate) {
            val rejectionMessage =
              s"P2P connectivity is not ready (authenticated = $authenticatedCount < dissemination quorum = $weakQuorum), rejecting"
            logger.info(rejectionMessage)
            from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
            metrics.ingress.labels.outcome.values.P2PNotReady
          } else if (mempoolState.receivedOrderRequests.sizeIs == config.maxQueueSize) {
            val rejectionMessage =
              s"mempool received client request but the queue is full (${config.maxQueueSize}), rejecting"
            logger.info(rejectionMessage)
            from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
            span.setStatus(StatusCode.ERROR, "queue_full"); span.end()
            metrics.ingress.labels.outcome.values.QueueFull
          } else if (config.checkTags && !orderingRequest.isTagValid) {
            val rejectionMessage =
              s"mempool received a client request with an invalid tag '${orderingRequest.tag}', " +
                s"valid tags are: (${OrderingRequest.ValidTags.mkString(", ")}), rejecting"
            logger.warn(rejectionMessage)
            from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
            span.setStatus(StatusCode.ERROR, "invalid_tag"); span.end()
            metrics.ingress.labels.outcome.values.InvalidTag
          } else {
            val payloadSize = orderingRequest.payload.size()
            if (payloadSize > config.maxRequestPayloadBytes) {
              val rejectionMessage =
                s"mempool received client request of size $payloadSize " +
                  s"but it exceeds the maximum (${config.maxRequestPayloadBytes}), rejecting"
              logger.warn(rejectionMessage)
              from.foreach(_.asyncSend(SequencerNode.RequestRejected(rejectionMessage)))
              span.setStatus(StatusCode.ERROR, "max_request_size_exceeded"); span.end()
              metrics.ingress.labels.outcome.values.RequestTooBig
            } else {
              mempoolState.receivedOrderRequests.enqueue((r, span))
              from.foreach(_.asyncSend(SequencerNode.RequestAccepted))
              if (mempoolState.receivedOrderRequests.sizeIs >= config.minRequestsInBatch.toInt) {
                // every time we receive a new transaction we only try to create new batches if we've reached
                // the configured minimum batch size. alternatively batches are also attempted creation on the configured
                // interval or when explicitly requested by availability
                createAndSendBatches()
              }
              emitStateStats(metrics, mempoolState)
              metrics.ingress.labels.outcome.values.Success
            }
          }
        emitRequestStats(metrics)(orderingRequest, sender, outcome, config.checkTags)

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

        createAndSendBatches()
        emitStateStats(metrics, mempoolState)

      case Mempool.MempoolBatchCreationClockTick =>
        logger.trace(
          s"Mempool received batch creation clock tick (maxRequestsInBatch: ${config.maxRequestsInBatch})"
        )
        createAndSendBatches()
        scheduleMempoolBatchCreationClockTick()

      case Mempool.P2PConnectivityUpdate(membership, authenticatedCountIncludingSelf) =>
        weakQuorum = membership.orderingTopology.weakQuorum
        authenticatedCount = authenticatedCountIncludingSelf
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
  private def createAndSendBatches()(implicit context: E#ActorContextT[Mempool.Message]): Unit =
    while (
      mempoolState.receivedOrderRequests.nonEmpty && mempoolState.toBeProvidedToAvailability > 0
    ) {
      mempoolState.toBeProvidedToAvailability -= 1
      createAndSendBatch()
      emitStateStats(metrics, mempoolState)
    }

  private def createAndSendBatch()(implicit context: E#ActorContextT[Mempool.Message]): Unit = {
    val requestsAndSpans = dequeueN(mempoolState.receivedOrderRequests, config.maxRequestsInBatch)
    val batchCreationInstant = Instant.now
    locally {
      val requests = requestsAndSpans.map(_._1.tx)
      implicit val traceContext = context.traceContextOfBatch(requests)
      emitRequestsQueuedForBatchInclusionLatencies(requests, batchCreationInstant)
      availability.asyncSend(Availability.LocalDissemination.LocalBatchCreated(requests))
    }
    requestsAndSpans.foreach(_._2.end())
    emitStateStats(metrics, mempoolState)
  }

  private def emitRequestsQueuedForBatchInclusionLatencies(
      requests: Seq[Traced[OrderingRequest]],
      batchCreationInstant: Instant,
  ): Unit = {
    import metrics.performance.orderingStageLatency.*
    requests.foreach(r =>
      emitOrderingStageLatency(
        labels.stage.values.mempool.RequestQueuedForBatchInclusion,
        r.value.orderingStartInstant,
        batchCreationInstant,
      )
    )
  }
}

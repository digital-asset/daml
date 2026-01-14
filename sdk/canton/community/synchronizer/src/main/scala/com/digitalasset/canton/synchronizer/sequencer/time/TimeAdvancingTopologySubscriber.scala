// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.config.TimeAdvancingTopologyConfig
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.{
  EffectiveTimeObservation,
  mkTimeAdvanceBroadcastMessageId,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, FutureUtil, LoggerUtil}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

/** To avoid flooding with time proofs from clients, we broadcast small messages from sequencers,
  * hoping that it will timely advance the sequencing time for members that are awaiting to observe
  * events.
  */
trait TimeAdvancingTopologySubscriber
    extends TopologyTransactionProcessingSubscriber
    with AutoCloseable

/** Sends a single time advancement broadcast for each topology transaction after the topology
  * change delay has elapsed on the local clock.
  *
  * This approach neither ensures that a advancement broadcast gets sequenced after the effective
  * time of a topology transactions nor is resilient to crashes. So with this approach, members of
  * the synchronizer must still request time proofs if they do not observe a broadcast message
  * within their patience.
  */
final class TimeAdvancingTopologySubscriberV1(
    clock: Clock,
    sequencerClient: SequencerClientSend,
    topologyClient: SynchronizerTopologyClientWithInit,
    psid: PhysicalSynchronizerId,
    thisSequencerId: SequencerId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TimeAdvancingTopologySubscriber
    with NamedLogging {

  private val protocolVersion = psid.protocolVersion

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (effectiveTimestamp.value > sequencedTimestamp.value) {

      for {
        // Conservatively, use a snapshot with topology changes that are active "now".
        snapshot <- topologyClient.currentSnapshotApproximation
        maybeSequencerGroup <- snapshot.sequencerGroup()
      } yield {
        val topologyChangeDelay = topologyClient.staticSynchronizerParameters.topologyChangeDelay
        maybeSequencerGroup.foreach { sequencerGroup =>
          if (sequencerGroup.active.contains(thisSequencerId)) {
            FutureUnlessShutdownUtil
              .doNotAwaitUnlessShutdown(
                clock
                  .scheduleAfter(
                    _ => broadcastToAdvanceTime(effectiveTimestamp),
                    // To become less prone to clock skew-related problems, wait for the topology change delay instead of
                    //  the effective time to elapse. This provides a better chance of sequencing and observing a broadcast
                    //  message before time proofs are triggered by sequencer clients.
                    //  However, sequencer client-triggered time proofs still remain as a fallback.
                    delta = topologyChangeDelay.duration,
                  ),
                failureMessage = "could not schedule a time-advancing message",
              )
          }
        }
      }
    } else {
      FutureUnlessShutdown.unit
    }

  @VisibleForTesting
  private[time] def broadcastToAdvanceTime(
      desiredTimestamp: EffectiveTime
  )(implicit traceContext: TraceContext): Unit = {
    implicit val metricsContext: MetricsContext = MetricsContext("type" -> "time-adv-broadcast")
    val batch =
      Batch.of(
        protocolVersion,
        Seq(
          TopologyTransactionsBroadcast(psid, Seq.empty) ->
            Recipients.cc(AllMembersOfSynchronizer)
        )*
      )

    val sendUS =
      for {
        // Ask for a topology snapshot again to avoid races on topology changes after scheduling.
        // This is only a best-effort check as the snapshot update may still happen concurrently.
        topologySnapshot <- topologyClient.currentSnapshotApproximation
        maybeSequencerGroup <- topologySnapshot.sequencerGroup()
        maybeAggregationRule = maybeSequencerGroup.flatMap { sequencerGroup =>
          NonEmpty.from(sequencerGroup.active).map { sequencerGroup =>
            AggregationRule(
              sequencerGroup,
              // We merely deduplicate here, so members eventually receive only one event; this means
              //  that the mechanism is not BFT, and we still rely on sequencer client-triggered time proofs
              //  for resilience against non-compliant sequencers.
              threshold = PositiveInt.one,
              protocolVersion,
            )
          }
        }
        _ <-
          if (maybeSequencerGroup.exists(_.active.contains(thisSequencerId))) {
            logger.debug(s"Sending a time-advancing message to hopefully reach $desiredTimestamp")
            sequencerClient
              .send(
                batch,
                topologyTimestamp = None,
                maxSequencingTime = desiredTimestamp.value
                  .plus(TimeAdvancingTopologyConfig.defaultMaxSequencingTimeWindow.asJava),
                aggregationRule = maybeAggregationRule,
                messageId = mkTimeAdvanceBroadcastMessageId(),
                callback = SendCallback.empty,
              )
              .valueOr(err =>
                LoggerUtil.logAtLevel(
                  SendAsyncClientError.logLevel(err),
                  s"Could not send a time-advancing message: $err",
                )
              )
          } else FutureUnlessShutdown.unit
      } yield ()

    FutureUtil.doNotAwait(
      sendUS.onShutdown(logger.debug("Time-advancing broadcast aborted on shutdown")),
      "Time advancing broadcast",
    )
  }

  override def close(): Unit = ()
}

/** Keeps track of outstanding effective times that have not yet been observed as sequenced time,
  * and regularly checks whether a time-advancing broadcast needs to be sent for any of them.
  *
  * The outstanding effective times are forgotten during a crash/restart or for newly onboarded
  * sequencer. Moreover, a broadcast message is sequenced only within the
  * [[com.digitalasset.canton.synchronizer.sequencer.config.TimeAdvancingTopologyConfig.maxSequencingTimeWindow]]
  * after the topology change's effective time. Therefore members can be configured to not request
  * time proofs upon topology changes if the synchronizer always has at least one honest,
  * non-crashed sequencer that is not freshly onboarded and there are no gaps in the sequenced block
  * times longer than the max sequencing time window.
  */
final class TimeAdvancingTopologySubscriberV2(
    clock: Clock,
    sequencerClient: SequencerClientSend,
    topologyClient: SynchronizerTopologyClientWithInit,
    psid: PhysicalSynchronizerId,
    thisSequencerId: SequencerId,
    broadcastTimeTracker: BroadcastTimeTracker,
    config: TimeAdvancingTopologyConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TimeAdvancingTopologySubscriber
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private val protocolVersion = psid.protocolVersion

  private val topologyChangeDelay: NonNegativeFiniteDuration =
    topologyClient.staticSynchronizerParameters.topologyChangeDelay

  private val outstandingEffectiveTimes
      : ConcurrentNavigableMap[EffectiveTime, EffectiveTimeObservation] =
    new ConcurrentSkipListMap[EffectiveTime, EffectiveTimeObservation](Ordering[EffectiveTime])

  TraceContext.withNewTraceContext("periodic check of time advancing topology subscriber") {
    implicit traceContext =>
      scheduleNextPeriodicCheck()
  }

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // We do not test whether transactions are non-empty, as this surprisingly breaks many tests:
    // The topology processing never becomes idle if we do this.
    // It is not clear to me (Andreas L) whether this behavior is needed.
    if (effectiveTimestamp.value > sequencedTimestamp.value) {
      val now = clock.now
      val observation = EffectiveTimeObservation(now)
      outstandingEffectiveTimes.put(effectiveTimestamp, observation).discard
    }
    FutureUnlessShutdown.unit
  }

  private def periodicCheck(
      now: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (isClosing) {
      logger.info("Stopping periodic checks due to shutdown of TimeAdvancingTopologySubscriber")
      FutureUnlessShutdown.unit
    } else {
      val broadcastBound = broadcastTimeTracker.lastBroadcastTimestamp
      logger.debug(
        s"Running periodic check for time-advancing broadcasts at $now. Lower bound: $broadcastBound"
      )
      dropUpToInclusive(EffectiveTime(broadcastBound))

      // Now iterate over everything that remains and determine whether there is a topology change that needs a resend
      val iterator = outstandingEffectiveTimes.entrySet().iterator()

      @tailrec def go(
          highestEffectiveTimeAndRetryCounter: Option[(EffectiveTime, Int)]
      ): Option[(EffectiveTime, Int)] =
        if (iterator.hasNext) {
          val nextEntry = iterator.next()
          val effectiveTime = nextEntry.getKey
          val observation = nextEntry.getValue
          needsABroadcastNow(now, observation) match {
            case None => go(highestEffectiveTimeAndRetryCounter)
            case Some(retryCounter) =>
              ErrorUtil.requireState(
                highestEffectiveTimeAndRetryCounter.forall { case (eff, _) => eff < effectiveTime },
                "ConcurrentSkipListMap iteration order violated",
              )
              // We simply pick the highest effective time that needs a broadcast.
              // This will in practice lead to the highest max sequencing times of all those that need a broadcast,
              // because upon a retry, we increment the max sequencing time by 1 microsecond,
              // which is the minimal difference between effective times anyway.
              go(Some(effectiveTime -> retryCounter))
          }
        } else highestEffectiveTimeAndRetryCounter

      go(None).foreach { case (effectiveTime, retriesSoFar) =>
        broadcastToAdvanceTime(now, effectiveTime, retriesSoFar)
      }

      scheduleNextPeriodicCheck()
      FutureUnlessShutdown.unit
    }

  private def needsABroadcastNow(
      now: CantonTimestamp,
      observation: EffectiveTimeObservation,
  ): Option[Int] = {
    val elapsedTimeSinceObserved = now - observation.observedAtLocal

    def needsATryNow(retryCounter: Int): Boolean =
      config.gracePeriod.asJava
        .plus(topologyChangeDelay.duration)
        .plus(config.sequencingPatience.asJava.multipliedBy(retryCounter.toLong))
        .compareTo(elapsedTimeSinceObserved) <= 0

    val oldRetryCounter = observation.retryCounter.getAndUpdate { current =>
      if (needsATryNow(current)) current + 1
      else current
    }
    Option.when(needsATryNow(oldRetryCounter))(oldRetryCounter)
  }

  private def scheduleNextPeriodicCheck()(implicit traceContext: TraceContext) =
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAfter(
        timestamp => periodicCheck(timestamp),
        config.pollBackoff.asJava,
      ),
      "Time advancing topology subscriber periodic check",
      closeContext = Some(this.closeContext),
    )

  private def dropUpToInclusive(effectiveTime: EffectiveTime): Unit =
    outstandingEffectiveTimes.headMap(effectiveTime, true).keySet().clear()

  @VisibleForTesting
  private[time] def broadcastToAdvanceTime(
      now: CantonTimestamp,
      desiredTimestamp: EffectiveTime,
      retryCounter: Int,
  )(implicit traceContext: TraceContext): Unit = {

    val sendUS = for {
      // Ask for a topology snapshot again to avoid races on topology changes after scheduling.
      topologySnapshot <- topologyClient.currentSnapshotApproximation
      maybeSequencerGroup <- topologySnapshot.sequencerGroup()
      _ <- maybeSequencerGroup match {
        case Some(sequencerGroup) if sequencerGroup.active.contains(thisSequencerId) =>
          val activeSequencers = NonEmptyUtil.fromUnsafe(sequencerGroup.active)
          val batch = Batch.of(
            protocolVersion,
            TopologyTransactionsBroadcast(psid, Seq.empty) -> Recipients
              .cc(AllMembersOfSynchronizer),
          )
          val aggregationRule = AggregationRule(
            activeSequencers,
            // We merely deduplicate here, so members eventually receive only one event; this means
            //  that the mechanism is not BFT, and we still rely on sequencer client-triggered time proofs
            //  for resilience against non-compliant sequencers.
            threshold = PositiveInt.one,
            protocolVersion,
          )
          val maxSequencingTime =
            desiredTimestamp.value
              .plus(config.maxSequencingTimeWindow.asJava)
              // Change the max sequencing time to avoid running in deduplication due to the aggregation rule.
              .addMicros(retryCounter.toLong)
          val messageId = mkTimeAdvanceBroadcastMessageId()
          logger.debug(
            s"At $now, sending a time-advancing message to hopefully reach $desiredTimestamp with max sequencing time $maxSequencingTime (message ID $messageId)"
          )
          implicit val metricsContext: MetricsContext =
            MetricsContext("type" -> "time-adv-broadcast")
          val sendResult = sequencerClient.send(
            batch,
            topologyTimestamp = None,
            maxSequencingTime = maxSequencingTime,
            aggregationRule = Some(aggregationRule),
            messageId = messageId,
            callback = SendCallback.empty,
          )
          sendResult.valueOr(err =>
            // Log only at INFO level because this sequencer might have been offboarded in between
            // and then we'd get errors here. Any other error will be retried anyway.
            logger.info(s"Could not send a time-advancing message: $err")
          )
        case _ =>
          // Do nothing if this is not an active sequencer
          FutureUnlessShutdown.unit
      }
    } yield ()

    FutureUtil.doNotAwait(
      sendUS.onShutdown(logger.debug("Time-advancing broadcast aborted on shutdown")),
      "Time advancing broadcast",
    )
  }
}

object TimeAdvancingTopologySubscriber {

  private[time] final case class EffectiveTimeObservation(observedAtLocal: CantonTimestamp) {
    val retryCounter: AtomicInteger = new AtomicInteger(0)
  }

  val TimeAdvanceBroadcastMessageIdPrefix: String = "time-adv-"

  private[time] def mkTimeAdvanceBroadcastMessageId(): MessageId =
    MessageId(
      String73.tryCreate(
        s"$TimeAdvanceBroadcastMessageIdPrefix${UUID.randomUUID()}",
        Some(TimeAdvanceBroadcastMessageIdPrefix + "message-id"),
      )
    )
}

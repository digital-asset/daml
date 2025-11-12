// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.{
  TimeAdvanceBroadcastMaxSequencingTimeWindow,
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
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, FutureUtil, LoggerUtil}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import scala.concurrent.ExecutionContext

/** To avoid flooding with time proofs from clients, we broadcast small messages from sequencers,
  * hoping that it will timely advance the sequencing time for members that are awaiting to observe
  * events.
  */
final class TimeAdvancingTopologySubscriber(
    clock: Clock,
    sequencerClient: SequencerClientSend,
    topologyClient: SynchronizerTopologyClientWithInit,
    psid: PhysicalSynchronizerId,
    thisSequencerId: SequencerId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with NamedLogging {

  private val protocolVersion = psid.protocolVersion

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (effectiveTimestamp.value > sequencedTimestamp.value) {
      // Conservatively, use a snapshot with topology changes that are active "now".
      val snapshot = topologyClient.currentSnapshotApproximation

      for {
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
      // Ask for a topology snapshot again to avoid races on topology changes after scheduling.
      topologyClient.currentSnapshotApproximation.sequencerGroup().flatMap { maybeSequencerGroup =>
        val maybeAggregationRule = maybeSequencerGroup.flatMap { sequencerGroup =>
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
        if (maybeSequencerGroup.exists(_.active.contains(thisSequencerId))) {
          logger.debug(s"Sending a time-advancing message to hopefully reach $desiredTimestamp")
          sequencerClient
            .send(
              batch,
              topologyTimestamp = None,
              maxSequencingTime =
                desiredTimestamp.value.plus(TimeAdvanceBroadcastMaxSequencingTimeWindow.duration),
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
      }

    FutureUtil.doNotAwait(
      sendUS.onShutdown(logger.debug("Time-advancing broadcast aborted on shutdown")),
      "Time advancing broadcast",
    )
  }
}

object TimeAdvancingTopologySubscriber {

  val TimeAdvanceBroadcastMaxSequencingTimeWindow: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(30)

  val TimeAdvanceBroadcastMessageIdPrefix: String = "time-adv-"

  private def mkTimeAdvanceBroadcastMessageId(): MessageId =
    MessageId(
      String73.tryCreate(
        s"$TimeAdvanceBroadcastMessageIdPrefix${UUID.randomUUID()}",
        Some(TimeAdvanceBroadcastMessageIdPrefix + "message-id"),
      )
    )
}

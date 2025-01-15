// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.data.OptionT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.TrafficConsumptionMetrics
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  GroupRecipient,
  SequencingSubmissionCost,
  TrafficState,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Maintains the current traffic state up to date for a given domain.
  */
class TrafficStateController(
    val member: Member,
    override val loggerFactory: NamedLoggerFactory,
    topologyClient: SyncCryptoClient[SyncCryptoApi],
    initialTrafficState: TrafficState,
    protocolVersion: ProtocolVersion,
    eventCostCalculator: EventCostCalculator,
    metrics: TrafficConsumptionMetrics,
    synchronizerId: SynchronizerId,
) extends NamedLogging {
  private val currentTrafficPurchased =
    new AtomicReference[Option[TrafficPurchased]](initialTrafficState.toTrafficPurchased(member))
  private val trafficConsumedManager = new TrafficConsumedManager(
    member,
    initialTrafficState.toTrafficConsumed(member),
    loggerFactory,
    metrics,
  )

  private implicit val memberMetricsContext: MetricsContext = MetricsContext(
    "member" -> member.toString,
    "domain" -> synchronizerId.toString,
  )

  def getTrafficConsumed: TrafficConsumed = trafficConsumedManager.getTrafficConsumed

  def getState: TrafficState = getTrafficConsumed.toTrafficState(currentTrafficPurchased.get())

  /** Update the traffic purchased entry for this member.
    * Only if the provided traffic purchased has a higher or equal serial number than the current traffic purchased.
    */
  def updateBalance(
      newTrafficPurchased: NonNegativeLong,
      serial: PositiveInt,
      timestamp: CantonTimestamp,
  )(implicit
      tc: TraceContext
  ): Unit = {
    val newState = currentTrafficPurchased.updateAndGet {
      case Some(old) if old.serial < serial =>
        Some(
          old.copy(
            extraTrafficPurchased = newTrafficPurchased,
            serial = serial,
            sequencingTimestamp = timestamp,
          )
        )
      case None =>
        Some(
          TrafficPurchased(
            member = member,
            serial = serial,
            extraTrafficPurchased = newTrafficPurchased,
            sequencingTimestamp = timestamp,
          )
        )
      case Some(other) =>
        logger.debug(
          s"Ignoring traffic purchased entry update with lower or equal serial number. Existing serial: ${other.serial}. Update serial: $serial."
        )
        Some(other)
    }
    newState.foreach(state =>
      metrics
        .extraTrafficPurchased(memberMetricsContext)
        .updateValue(state.extraTrafficPurchased.value)
    )
    logger.debug(s"Updating traffic purchased entry $newState")
  }

  /** Used when we receive a deliver error receipt for an event that did not consume traffic.
    * It will still update the traffic state to reflect the base traffic remainder at the provided timestamp.
    */
  def tickStateAt(sequencingTimestamp: CantonTimestamp)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Unit = FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
    for {
      topology <- topologyClient.awaitSnapshot(sequencingTimestamp)
      snapshot = topology.ipsSnapshot
      trafficControlO <- snapshot.trafficControlParameters(protocolVersion)
    } yield trafficControlO.foreach { params =>
      val updated = trafficConsumedManager.updateAt(sequencingTimestamp, params, logger)

      if (updated.sequencingTimestamp != sequencingTimestamp)
        logger.debug(
          "Skipped traffic update because the current state is more recent than the sequenced event." +
            s"Event timestamp: $sequencingTimestamp. Current state: $updated"
        )
      else
        logger.debug(
          s"Updated traffic state at timestamp: $sequencingTimestamp without consuming traffic. Current state: $updated"
        )
    },
    s"Failed to update traffic consumed state at $sequencingTimestamp",
  )

  def updateWithReceipt(
      trafficReceipt: TrafficReceipt,
      timestamp: CantonTimestamp,
      deliverErrorReason: Option[String],
      eventSpecificMetricsContext: MetricsContext,
  ): Unit =
    // Only update the event-specific traffic metrics if the state was updated (to avoid double reporting cost consumption)
    // This is especially relevant during crash recovery if the member replays events from the sequencer subscription
    if (trafficConsumedManager.updateWithReceipt(trafficReceipt, timestamp)) {
      // For event specific metrics, we merge the member metrics context with the metrics context containing
      // additional labels such as application ID and event type. It's possible we don't have such context
      // during crash recovery though as this context is kept in the send tracker in memory cache, which is not persisted
      val mergedEventSpecificMetricsContext =
        memberMetricsContext.merge(eventSpecificMetricsContext)
      deliverErrorReason match {
        case Some(reason) =>
          metrics.trafficCostOfNotDeliveredSequencedEvent.mark(trafficReceipt.consumedCost.value)(
            mergedEventSpecificMetricsContext.withExtraLabels("reason" -> reason)
          )
          metrics.deliveredEventCounter.inc()(mergedEventSpecificMetricsContext)
        case None =>
          metrics.trafficCostOfDeliveredSequencedEvent.mark(trafficReceipt.consumedCost.value)(
            mergedEventSpecificMetricsContext
          )
          metrics.rejectedEventCounter.inc()(mergedEventSpecificMetricsContext)
      }
    }

  /** Compute the cost of a batch of envelopes.
    * Does NOT debit the cost from the current traffic purchased.
    */
  def computeCost(
      batch: Batch[DefaultOpenEnvelope],
      snapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Option[SequencingSubmissionCost]] = {
    val groups =
      batch.envelopes.flatMap(_.recipients.allRecipients).collect { case g: GroupRecipient => g }
    val costFO = for {
      trafficControl <- OptionT(
        snapshot.trafficControlParameters(
          protocolVersion,
          warnOnUsingDefault = false,
        )
      )
      groupToMembers <- OptionT
        .liftF(
          GroupAddressResolver.resolveGroupsToMembers(groups.toSet, snapshot)
        )
    } yield {
      val costDetails = eventCostCalculator.computeEventCost(
        batch.map(_.closeEnvelope),
        trafficControl.readVsWriteScalingFactor,
        groupToMembers,
        protocolVersion,
      )
      logger.debug(
        s"Computed following cost for submission request using topology at ${snapshot.timestamp}: $costDetails"
      )
      costDetails.eventCost
    }

    costFO.value.map {
      _.map { cost =>
        SequencingSubmissionCost(
          cost,
          protocolVersion,
        )
      }
    }
  }
}

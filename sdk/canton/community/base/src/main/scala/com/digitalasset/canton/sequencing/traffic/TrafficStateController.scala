// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.data.OptionT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  GroupRecipient,
  SequencingSubmissionCost,
  TrafficState,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
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
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
) extends NamedLogging {
  private val currentTrafficPurchased =
    new AtomicReference[Option[TrafficPurchased]](initialTrafficState.toTrafficPurchased(member))
  private val trafficConsumedManager = new TrafficConsumedManager(
    member,
    initialTrafficState.toTrafficConsumed(member),
    loggerFactory,
  )

  // Use a queue to process the incoming events in order while not blocking the sequencer client on continuing its own event processing.
  private val consumeEventsQueue = new SimpleExecutionQueue(
    "consume-traffic-queue",
    futureSupervisor = futureSupervisor,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
    logTaskTiming = true,
  )

  def getTrafficConsumed: TrafficConsumed = trafficConsumedManager.getTrafficConsumed

  /** Update the traffic purchased entry for this member.
    * Only if the provided traffic purchased has a higher or equal serial number than the current traffic purchased.
    */
  def updateBalance(newBalance: NonNegativeLong, serial: PositiveInt, timestamp: CantonTimestamp)(
      implicit tc: TraceContext
  ): Unit = {
    val newState = currentTrafficPurchased.updateAndGet {
      case Some(old) if old.serial < serial =>
        Some(
          old.copy(
            extraTrafficPurchased = newBalance,
            serial = serial,
            sequencingTimestamp = timestamp,
          )
        )
      case None =>
        Some(
          TrafficPurchased(
            member = member,
            serial = serial,
            extraTrafficPurchased = newBalance,
            sequencingTimestamp = timestamp,
          )
        )
      case Some(other) =>
        logger.debug(
          s"Ignoring traffic purchased entry update with lower or equal serial number. Existing serial: ${other.serial}. Update serial: $serial."
        )
        Some(other)
    }
    logger.debug(s"Updating traffic purchased entry $newState")
  }

  /** Consume the cost from the current traffic purchased. Does not perform any check related to available credits.
    * @param submissionCost cost to be debited.
    * @param sequencingTimestamp sequencing timestamp of the event that incurred the cost.
    */
  def consume(submissionCost: SequencingSubmissionCost, sequencingTimestamp: CantonTimestamp)(
      implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Unit = {
    FutureUtil.doNotAwaitUnlessShutdown(
      consumeEventsQueue.executeUS(
        {
          for {
            topology <- topologyClient.awaitSnapshotUS(sequencingTimestamp)
            snapshot = topology.ipsSnapshot
            trafficControlO <- snapshot.trafficControlParameters(protocolVersion)
          } yield trafficControlO.foreach { params =>
            trafficConsumedManager.doConsumeAt(params, submissionCost.cost, sequencingTimestamp)
          }
        },
        s"Consuming traffic cost ${submissionCost.cost.value} for member $member at $sequencingTimestamp.",
      ),
      s"Failed to consume traffic cost ${submissionCost.cost.value} for member $member at $sequencingTimestamp.",
    )
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
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield eventCostCalculator.computeEventCost(
      batch.map(_.closeEnvelope),
      trafficControl.readVsWriteScalingFactor,
      groupToMembers,
      protocolVersion,
    )

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

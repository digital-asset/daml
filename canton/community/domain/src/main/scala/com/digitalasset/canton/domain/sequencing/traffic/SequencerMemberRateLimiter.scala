// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.implicits.showInterpolator
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  TrafficState,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.EventCostCalculator
import monocle.macros.syntax.lens.*

class SequencerMemberRateLimiter(
    member: Member,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    eventCostCalculator: EventCostCalculator = new EventCostCalculator(),
) extends NamedLogging {

  /** Consume the traffic costs of the event from the sequencer members traffic state.
    *
    * NOTE: This method must be called in order of the sequencing timestamps.
    */
  def tryConsume(
      event: Batch[ClosedEnvelope],
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      trafficState: TrafficState,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      currentBalance: NonNegativeLong,
  )(implicit
      tc: TraceContext
  ): (
    Either[SequencerRateLimitError.AboveTrafficLimit, TrafficState],
  ) = {
    require(
      sequencingTimestamp > trafficState.timestamp,
      show"Tried to consume an events out of order. Last event at ${trafficState.timestamp}, new event at $sequencingTimestamp",
    )

    val eventCost = eventCostCalculator.computeEventCost(
      event,
      trafficControlConfig.readVsWriteScalingFactor,
      groupToMembers,
    )

    val (newTrafficState, accepted) =
      updateTrafficState(
        sequencingTimestamp,
        trafficControlConfig,
        eventCost,
        trafficState,
        currentBalance,
      )

    logger.debug(s"Updated traffic status for $member: $newTrafficState")

    if (accepted)
      Right(newTrafficState)
    else {
      Left(
        SequencerRateLimitError.AboveTrafficLimit(
          member = member,
          trafficCost = eventCost,
          newTrafficState,
        )
      )
    }
  }

  private[traffic] def updateTrafficState(
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      eventCost: NonNegativeLong,
      trafficState: TrafficState,
      currentBalance: NonNegativeLong,
  )(implicit tc: TraceContext) = {
    // Get the traffic limit for this event and prune the in memory queue until then, since we won't need earlier top ups anymore
    require(
      currentBalance >= trafficState.extraTrafficConsumed,
      s"Extra traffic limit at $sequencingTimestamp (${currentBalance.value}) is < to extra traffic consumed (${trafficState.extraTrafficConsumed})",
    )
    val newExtraTrafficRemainder = currentBalance.value - trafficState.extraTrafficConsumed.value

    // determine the time elapsed since we approved last time
    val deltaMicros = sequencingTimestamp.toMicros - trafficState.timestamp.toMicros
    val trafficAllowedSinceLastTimestamp = Math
      .floor(trafficControlConfig.baseRate.value * deltaMicros.toDouble / 1e6)
      .toLong

    // determine the whole number of bytes that we were allowed to submit in that period
    // That is accumulated traffic from latest allowed update + whatever was left at the time,
    // upper-bounded by maxBurst
    val allowedByteFromBaseRate =
      Math.min(
        trafficControlConfig.maxBaseTrafficAmount.value,
        trafficAllowedSinceLastTimestamp + trafficState.baseTrafficRemainder.value,
      )

    if (allowedByteFromBaseRate == trafficControlConfig.maxBaseTrafficAmount.value)
      logger.trace(
        s"Member $member reached their max burst limit of $allowedByteFromBaseRate at $sequencingTimestamp"
      )

    // If we're below the allowed base rate limit, just consume from that
    val (newState, accepted) = if (eventCost.value <= allowedByteFromBaseRate) {
      trafficState
        .focus(_.extraTrafficRemainder)
        .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder))
        .focus(_.baseTrafficRemainder)
        .replace(NonNegativeLong.tryCreate(allowedByteFromBaseRate - eventCost.value))
        .focus(_.timestamp)
        .replace(sequencingTimestamp) -> true

      // Otherwise...
    } else {
      // The extra traffic needed is the event cost minus what we can use from base rate
      val extraTrafficConsumed =
        NonNegativeLong.tryCreate(eventCost.value - allowedByteFromBaseRate)
      // ...if that's below how much extra traffic we have, keep going
      if (extraTrafficConsumed.value <= newExtraTrafficRemainder) {

        trafficState
          // Set the base traffic remainder to zero since we need to use all of it
          .focus(_.baseTrafficRemainder)
          .replace(NonNegativeLong.zero)
          // Increase the extra traffic consumed
          .focus(_.extraTrafficConsumed)
          .modify(_ + extraTrafficConsumed)
          // Replace the old remainder with new remainder - extra traffic consumed
          .focus(_.extraTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder - extraTrafficConsumed.value))
          .focus(_.timestamp)
          .replace(sequencingTimestamp) -> true

      } else {
        // If not, update the remaining base and extra traffic but deny the event
        trafficState
          .focus(_.extraTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder))
          .focus(_.baseTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(allowedByteFromBaseRate))
          .focus(_.timestamp)
          .replace(sequencingTimestamp) -> false
      }
    }

    if (accepted) metrics.trafficControl.eventDelivered.mark(eventCost.value)(MetricsContext.Empty)
    else metrics.trafficControl.eventRejected.mark(eventCost.value)(MetricsContext.Empty)

    (newState, accepted)
  }
}

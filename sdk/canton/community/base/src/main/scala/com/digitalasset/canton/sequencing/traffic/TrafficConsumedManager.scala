// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference

/** Holds the traffic consumed state of a member.
  * This is used by the sequencer to keep track of the traffic consumed by its members,
  * as well as by the members themselves in the TrafficStateController to keep track of their own traffic consumed.
  */
class TrafficConsumedManager(
    val member: Member,
    initValue: TrafficConsumed,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val trafficConsumed = new AtomicReference[TrafficConsumed](initValue)

  def getTrafficConsumed: TrafficConsumed = trafficConsumed.get

  /** Consume the event cost at the given timestamp.
    */
  def doConsumeAt(
      params: TrafficControlParameters,
      eventCost: NonNegativeLong,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): TrafficConsumed = {
    val newState = trafficConsumed.updateAndGet {
      _.consume(timestamp, params, eventCost)
        .valueOr(e =>
          ErrorUtil.invalidState(
            s"Failed to consume traffic for $member at $timestamp with cost ${eventCost.value}: $e"
          )
        )
    }

    logger.debug(s"Consumed ${eventCost.value} for $member at $timestamp: new state $newState")

    newState
  }
}

object TrafficConsumedManager {
  final case class NotEnoughTraffic(
      member: Member,
      cost: NonNegativeLong,
      trafficState: TrafficState,
  )
}

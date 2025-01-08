// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.TrafficConsumptionMetrics
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.sequencing.traffic.TrafficConsumedManager.NotEnoughTraffic
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

/** Holds the traffic consumed state of a member.
  * This is used by the sequencer to keep track of the traffic consumed by its members,
  * as well as by the members themselves in the TrafficStateController to keep track of their own traffic consumed.
  */
class TrafficConsumedManager(
    val member: Member,
    initValue: TrafficConsumed,
    override val loggerFactory: NamedLoggerFactory,
    metrics: TrafficConsumptionMetrics,
) extends NamedLogging {

  private val trafficConsumed = new AtomicReference[TrafficConsumed](initValue)

  def getTrafficConsumed: TrafficConsumed = trafficConsumed.get

  /** Update the traffic consumed state with the provided receipt only if it is more recent.
    * @return true if the state was updated.
    */
  def updateWithReceipt(trafficReceipt: TrafficReceipt, timestamp: CantonTimestamp)(implicit
      metricsContext: MetricsContext
  ): Boolean = {
    // We need to get the traffic consumed before the update to compare its timestamp with the input
    // timestamp and know for sure if it was updated or not. That's why we don't use "updateAndGet" here
    // but instead trafficConsumed.getAndUpdate
    val oldTrafficConsumed = trafficConsumed.getAndUpdate {
      case current if current.sequencingTimestamp < timestamp =>
        current.copy(
          extraTrafficConsumed = trafficReceipt.extraTrafficConsumed,
          baseTrafficRemainder = trafficReceipt.baseTrafficRemainder,
          lastConsumedCost = trafficReceipt.consumedCost,
          sequencingTimestamp = timestamp,
        )
      case current => current
    }

    val trafficConsumedUpdated = oldTrafficConsumed.sequencingTimestamp < timestamp

    // And then update the metrics if the state was updated
    if (trafficConsumedUpdated) {
      updateTrafficConsumedMetrics(
        trafficReceipt.extraTrafficConsumed,
        trafficReceipt.baseTrafficRemainder,
        timestamp,
      )
    }

    trafficConsumedUpdated
  }

  /** Validate that the event cost is below the traffic limit at the provided timestamp.
    * DOES NOT debit the cost from the traffic state.
    */
  def canConsumeAt(
      params: TrafficControlParameters,
      cost: NonNegativeLong,
      timestamp: CantonTimestamp,
      trafficPurchasedO: Option[TrafficPurchased],
  )(implicit traceContext: TraceContext): Either[NotEnoughTraffic, Unit] =
    trafficConsumed
      .get()
      .canConsumeAt(params, cost, timestamp, trafficPurchasedO, logger)

  /** Update the traffic consumed state to the given timestamp, including updating base rate remainder,
    * ONLY if it's not already up to date.
    */
  def updateAt(timestamp: CantonTimestamp, params: TrafficControlParameters, logger: TracedLogger)(
      implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ) =
    updateAndGet {
      case trafficConsumed if trafficConsumed.sequencingTimestamp < timestamp =>
        trafficConsumed.updateTimestamp(timestamp, params, logger)
      case trafficConsumed => trafficConsumed
    }

  /** Consume the event cost at the given timestamp if enough traffic is available.
    * This MUST be called sequentially.
    */
  def consumeIfEnoughTraffic(
      params: TrafficControlParameters,
      eventCost: NonNegativeLong,
      timestamp: CantonTimestamp,
      trafficPurchasedO: Option[TrafficPurchased],
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): Either[NotEnoughTraffic, TrafficConsumed] =
    canConsumeAt(
      params,
      eventCost,
      timestamp,
      trafficPurchasedO,
    ) match {
      case Left(value) =>
        updateAndGet {
          _.updateTimestamp(timestamp, params, logger)
        }.discard
        Left(value)
      case Right(_) =>
        val newState = updateAndGet {
          _.consume(timestamp, params, eventCost, logger)
        }
        logger.debug(s"Consumed ${eventCost.value} for $member at $timestamp: new state $newState")
        Right(newState)
    }

  // Single point of entry to update the traffic consumed
  // Update metrics as a side effect. f itself MUST NOT have side effects.
  private def updateAndGet(
      f: UnaryOperator[TrafficConsumed]
  )(implicit metricsContext: MetricsContext) = {
    val newTrafficConsumed = trafficConsumed.updateAndGet(f)
    updateTrafficConsumedMetrics(
      newTrafficConsumed.extraTrafficConsumed,
      newTrafficConsumed.baseTrafficRemainder,
      newTrafficConsumed.sequencingTimestamp,
    )
    newTrafficConsumed
  }

  private def updateTrafficConsumedMetrics(
      extraTrafficConsumed: NonNegativeLong,
      baseTrafficRemainder: NonNegativeLong,
      lastTrafficUpdateTimestamp: CantonTimestamp,
  )(implicit metricsContext: MetricsContext): Unit = {
    metrics
      .extraTrafficConsumed(metricsContext)
      .updateValue(extraTrafficConsumed.value)
    metrics
      .baseTrafficRemainder(metricsContext)
      .updateValue(baseTrafficRemainder.value)
    metrics
      .lastTrafficUpdateTimestamp(metricsContext)
      .updateValue(lastTrafficUpdateTimestamp.getEpochSecond)
  }
}

object TrafficConsumedManager {
  final case class NotEnoughTraffic(
      member: Member,
      cost: NonNegativeLong,
      trafficState: TrafficState,
  )
}

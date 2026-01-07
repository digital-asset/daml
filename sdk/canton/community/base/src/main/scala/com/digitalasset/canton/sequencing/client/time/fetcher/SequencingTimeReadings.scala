// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeReadings.TimeReading
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

private[client] trait SequencingTimeReadings {

  def getTimeReadings(
      maxTimeReadingsAge: Option[NonNegativeFiniteDuration]
  ): Map[SequencerId, TimeReading]

  def validTimeInterval(
      times: Vector[CantonTimestamp],
      trustThreshold: PositiveInt,
  )(implicit traceContext: TraceContext): Option[(CantonTimestamp, CantonTimestamp)]

  def recordReading(
      sequencerId: SequencerId,
      timeReading: Option[CantonTimestamp],
      receivedAt: CantonTimestamp,
  ): Unit
}

object SequencingTimeReadings {

  private[client] final case class TimeReading(
      reading: Option[CantonTimestamp],
      receivedAt: CantonTimestamp,
  )
}

private[client] class ExpiringInMemorySequencingTimeReadings(
    localClock: Clock,
    timeReadingsRetention: PositiveFiniteDuration,
    override protected val loggerFactory: NamedLoggerFactory,
    timesRef: AtomicReference[Map[SequencerId, TimeReading]] = // Only for testing
      new AtomicReference(Map.empty),
) extends SequencingTimeReadings
    with NamedLogging {

  import ExpiringInMemorySequencingTimeReadings.*

  override def getTimeReadings(
      maxTimeReadingsAge: Option[NonNegativeFiniteDuration]
  ): Map[SequencerId, TimeReading] = {
    val now = localClock.now
    timesRef
      .updateAndGet { times =>
        times.filter { case (_, TimeReading(_, receivedAt)) =>
          now.minus(timeReadingsRetention.duration) < receivedAt
        }
      }
      .filter { case (_, TimeReading(_, receivedAt)) =>
        maxTimeReadingsAge.forall(age => now.minus(age.duration) < receivedAt)
      }
  }

  def validTimeInterval(
      times: Vector[CantonTimestamp],
      trustThreshold: PositiveInt,
  )(implicit traceContext: TraceContext): Option[(CantonTimestamp, CantonTimestamp)] = {
    val bftTimeThresholdInt = bftTimeThreshold(trustThreshold).unwrap
    val result =
      Option.when(times.sizeIs >= bftTimeThresholdInt) {
        // We expect a low cardinality, so using sorting is fine
        val sortedTimes = times.sorted
        sortedTimes(trustThreshold.unwrap - 1) ->
          sortedTimes(sortedTimes.size - trustThreshold.unwrap)
      }
    if (result.isEmpty)
      logger.debug(
        s"Cannot determine sequencing time: only ${times.size} times have been provided but " +
          s"at least $bftTimeThresholdInt are needed"
      )
    result
  }

  def recordReading(
      sequencerId: SequencerId,
      timeReading: Option[CantonTimestamp],
      receivedAt: CantonTimestamp,
  ): Unit =
    timesRef
      .updateAndGet(
        _.updatedWith(sequencerId)(v =>
          v.map { case TimeReading(prevTimeReading, prevReadingReceivedAt) =>
            (prevTimeReading, timeReading) match {
              case (Some(prevTs), Some(ts)) =>
                TimeReading(
                  Some(prevTs max ts),
                  if (prevTs > ts) prevReadingReceivedAt else receivedAt,
                )
              case (Some(_), None) =>
                TimeReading(prevTimeReading, prevReadingReceivedAt)
              case (None, Some(_)) =>
                TimeReading(timeReading, receivedAt)
              case (None, None) =>
                TimeReading(None, prevReadingReceivedAt)
            }
          }.orElse(Some(TimeReading(timeReading, receivedAt)))
        )
      )
      .discard
}

private object ExpiringInMemorySequencingTimeReadings {

  private def bftTimeThreshold(trustThreshold: PositiveInt): PositiveInt =
    trustThreshold + trustThreshold.decrement
}

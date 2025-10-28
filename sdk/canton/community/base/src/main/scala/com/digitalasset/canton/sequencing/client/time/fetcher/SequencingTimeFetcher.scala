// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeFetcher.*
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeReadings.TimeReading
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration
import scala.concurrent.ExecutionContext

/** Allows to determine the current sequencing time, and whether a sequencing time has been reached,
  * based on a set of time sources, caching their results to avoid repeated calls to the same time
  * source.
  */
class SequencingTimeFetcher private[client] (
    timeSourcesPool: TimeSourcesPool,
    timeSourcesAccessor: TimeSourcesAccessor,
    localClock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val timeReadings = timeSourcesAccessor.timeReadings

  def currentSequencingTimeInfo(maxTimeReadingsAge: Option[NonNegativeFiniteDuration])(implicit
      traceContext: TraceContext
  ): SequencingTimeInfo = {
    val readings = timeReadings.getTimeReadings(maxTimeReadingsAge)
    val times = readings.view.values.flatMap(_.reading).toVector
    val trustThreshold = timeSourcesPool.readTrustThreshold()
    SequencingTimeInfo(
      timeReadings.validTimeInterval(times, trustThreshold),
      trustThreshold,
      readings,
    )
  }

  /** Returns `true` only if synchronizer has surely reached the given sequencing time.
    */
  // Given a sequencing time, we want to know if at least one non-faulty sequencer node has reached it
  //  (i.e., if any subsequent send will be assigned a later sequencing time).
  //
  //  A negative outcome is however considered safe because in that case we'll retry, so false negatives
  //  are not a correctness problem; this means that a large number of unresponsive nodes can be tolerated.
  //
  //  Let `f` = maximum tolerated number of nodes that can be faulty in arbitrary ways, i.e., `trustThreshold` - 1.
  //
  //  In order to make sure that at least one non-faulty node has reached the given time, we must find
  //  at least additional `f` nodes that have also reached it, i.e., `f+1` in total.
  //  This is because in the worst case `f` nodes may be maliciously colluding and causing a false positive answer
  //  (e.g., their `GetTime` could always return `CantonTimestamp.MaxValue`), and in that case we need
  //  another `f` replies agreeing with the correct node to outvote the malicious ones.
  //
  //  This means that in the worst case, under the trust assumption of up to `f` non-compliant nodes,
  //  we may end up querying `2f+1` nodes.
  //
  //  Since connections are not exclusive, however, we can just ask for one connection per sequencer
  //  and have the best possible pool at our disposal, so our strategy is to try and reach `f+1` positive
  //  outcomes, potentially by ending up querying all available sequencers.
  //  This allows tolerating a large number of unresponsive nodes.
  def hasReached(
      time: CantonTimestamp,
      timeout: PositiveFiniteDuration,
      maxTimeReadingsAge: Option[NonNegativeFiniteDuration] = None,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = {
    def returnOutcome(outcome: Boolean) =
      FutureUnlessShutdown.pure(outcome)

    def countPositives(times: Map[SequencerId, TimeReading]): Int =
      times.values.count(_.reading.exists(_ >= time))

    logger.debug(s"Asked whether synchronizer has reached sequencing time $time")
    val times = timeReadings.getTimeReadings(maxTimeReadingsAge)
    logger.debug("Available cached time readings: " + times)
    val positives = countPositives(times)
    val trustThreshold = timeSourcesPool.readTrustThreshold()
    val trustThresholdInt = trustThreshold.unwrap

    if (positives >= trustThresholdInt) {
      logger.debug(s"The synchronizer has reached sequencing time $time (cached)")
      returnOutcome(true)
    } else {
      val missingPositives = trustThresholdInt - positives
      val timeSources =
        getTimeSources(
          exclusions = times.keySet,
          missingPositives,
        )
      if (timeSources.sizeIs < missingPositives) {
        logger.debug(
          s"Cannot determine whether the synchronizer has reached $time: insufficient threshold " +
            s"(missing positives: $missingPositives, available time sources: ${timeSources.size})"
        )
        returnOutcome(false)
      } else {
        val start = localClock.now
        timeSourcesAccessor
          .queryTimeSources(timeSources, timeout)
          .flatMap { _ =>
            val duration = localClock.now - start
            val positives = countPositives(timeReadings.getTimeReadings(maxTimeReadingsAge = None))
            if (positives >= trustThresholdInt) {
              logger.debug(
                s"The synchronizer has reached sequencing time $time " +
                  s"with $positives positives (threshold: $trustThresholdInt)"
              )
              returnOutcome(true)
            } else {
              val leftTime = timeout.duration.minus(duration)
              if (leftTime.compareTo(Duration.ZERO) > 0) {
                logger.debug(
                  s"Cannot determine yet whether the synchronizer has reached sequencing time $time " +
                    s"due to insufficient $positives positives (threshold: $trustThresholdInt), trying more sources"
                )
                hasReached(
                  time,
                  PositiveFiniteDuration.tryCreate(leftTime),
                  maxTimeReadingsAge = None,
                )
              } else {
                logger.debug(
                  s"Cannot determine yet whether the synchronizer has reached sequencing time $time " +
                    s"due to insufficient $positives positives (threshold: $trustThresholdInt) and the time is over"
                )
                returnOutcome(false)
              }
            }
          }
      }
    }
  }

  private def getTimeSources(exclusions: Set[SequencerId], numberOfTimesToFetch: Int)(implicit
      traceContext: TraceContext
  ): Map[SequencerId, PositiveFiniteDuration => FutureUnlessShutdown[Option[CantonTimestamp]]] =
    PositiveInt
      .create(numberOfTimesToFetch)
      .map(
        timeSourcesPool.timeSources(_, exclusions).toMap
      )
      .getOrElse(Map.empty)
}

object SequencingTimeFetcher {

  private[client] trait TimeSourcesPool {

    def readTrustThreshold(): PositiveInt

    def timeSources(count: PositiveInt, exclusions: Set[SequencerId])(implicit
        traceContext: TraceContext
    ): Seq[(SequencerId, PositiveFiniteDuration => FutureUnlessShutdown[Option[CantonTimestamp]])]
  }

  private[client] final case class SequencingTimeInfo(
      validTimeInterval: Option[(CantonTimestamp, CantonTimestamp)],
      forTrustThreshold: PositiveInt,
      basedOnTimeReadings: Map[SequencerId, TimeReading],
  )
}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.time.fetcher.OneCallAtATimeSourcesAccessor.QueryTimeSourcesRunningTask
import com.digitalasset.canton.sequencing.client.time.fetcher.TimeSourcesAccessor.TimeSources
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.View
import scala.concurrent.ExecutionContext

private[client] trait TimeSourcesAccessor {

  def timeReadings: SequencingTimeReadings

  def queryTimeSources(
      timeSources: TimeSources,
      timeout: PositiveFiniteDuration,
      concurrent: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, Option[CantonTimestamp]]]
}

object TimeSourcesAccessor {

  private[client] type TimeSources =
    Map[SequencerId, PositiveFiniteDuration => FutureUnlessShutdown[Option[CantonTimestamp]]]
}

private[client] class OneCallAtATimeSourcesAccessor(
    localClock: Clock,
    override val timeReadings: SequencingTimeReadings,
    override protected val loggerFactory: NamedLoggerFactory,
    runningTaskRef: AtomicReference[Option[QueryTimeSourcesRunningTask]] = // Only for testing
      new AtomicReference(None),
)(implicit
    ec: ExecutionContext
) extends TimeSourcesAccessor
    with NamedLogging {

  override def queryTimeSources(
      timeSources: TimeSources,
      timeout: PositiveFiniteDuration,
      concurrent: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, Option[CantonTimestamp]]] = {

    def updateStatusAfterCompletion(completedSequencerIds: Set[SequencerId]): Unit =
      runningTaskRef.getAndUpdate {
        case Some((ids, fut)) =>
          val remainingIds = ids.diff(completedSequencerIds)
          Option.when(remainingIds.nonEmpty)((remainingIds, fut))
        case None => None
      }.discard

    val completionPromise =
      PromiseUnlessShutdown.unsupervised[Map[SequencerId, Option[CantonTimestamp]]]()

    val previousState =
      if (concurrent)
        None
      else {
        runningTaskRef.getAndUpdate {
          case None => Some((timeSources.keySet, completionPromise.futureUS))
          case Some(runningSequencerIds -> fut) =>
            Some((runningSequencerIds ++ timeSources.keySet, fut))
        }
      }

    val queryF =
      previousState match {
        case Some((runningSequencerIds, fut)) =>
          val prevCompletionOrTimeoutPromise =
            PromiseUnlessShutdown.unsupervised[Map[SequencerId, Option[CantonTimestamp]]]()
          val shouldSetPromise = new AtomicBoolean(false)
          val sequencerIdsToStart = timeSources.keySet.diff(runningSequencerIds)
          def setFirst(r: Map[SequencerId, Option[CantonTimestamp]]): Unit =
            if (shouldSetPromise.compareAndSet(false, true))
              prevCompletionOrTimeoutPromise.outcome_(r)
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            fut.map(r => setFirst(r)),
            "waiting for ongoing time source query failed",
          )
          // It's OK not to return the result from the running computation if we time out,
          //  because we won't retry in that case
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            localClock
              .scheduleAfter(_ => setFirst(Map.empty), timeout.duration),
            "waiting for timeout to elapse failed",
          )
          val timeSourcesToBeStarted =
            sequencerIdsToStart.view
              .flatMap(sequencerId => timeSources.get(sequencerId).map(sequencerId -> _))
          logger
            .debug(s"Querying additional time sources: $sequencerIdsToStart")
          val futures = startTimeSources(timeSourcesToBeStarted, timeout)
          prevCompletionOrTimeoutPromise.futureUS.flatMap { result1 =>
            futures
              .parTraverse(recordReadingOnceComplete)
              .map { result2 =>
                val aggregatedResult = result1 ++ result2.toMap
                updateStatusAfterCompletion(completedSequencerIds = sequencerIdsToStart)
                completionPromise.outcome_(aggregatedResult)
              }
          }

        case None =>
          logger.debug(s"Querying time sources: ${timeSources.keySet}")
          val futures = startTimeSources(timeSources.view, timeout)
          futures
            .parTraverse(recordReadingOnceComplete)
            .map { result =>
              updateStatusAfterCompletion(completedSequencerIds = timeSources.keySet)
              completionPromise.outcome_(result.toMap)
            }
      }

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(queryF, "Querying time sources failed")

    completionPromise.futureUS
  }

  private def startTimeSources(
      timeSourcesToBeStarted: View[
        (SequencerId, PositiveFiniteDuration => FutureUnlessShutdown[Option[CantonTimestamp]])
      ],
      timeout: PositiveFiniteDuration,
  ): Seq[FutureUnlessShutdown[(SequencerId, Option[CantonTimestamp])]] =
    timeSourcesToBeStarted.map { case (sequencerId, timeSource) =>
      timeSource(timeout).map(sequencerId -> _)
    }.toSeq

  private def recordReadingOnceComplete(
      fut: FutureUnlessShutdown[(SequencerId, Option[CantonTimestamp])]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(SequencerId, Option[CantonTimestamp])] =
    fut.map { case (sequencerId, timestampO) =>
      logger.debug(s"Received response from time source $sequencerId: $timestampO")
      timeReadings.recordReading(sequencerId, timestampO, localClock.now)
      sequencerId -> timestampO
    }
}

private object OneCallAtATimeSourcesAccessor {

  type QueryTimeSourcesRunningTask =
    (Set[SequencerId], FutureUnlessShutdown[Map[SequencerId, Option[CantonTimestamp]]])
}

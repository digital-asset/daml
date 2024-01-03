// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.HasFlushFuture
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*

/** Periodically pull the latest clean timestamp and if it has changed acknowledge it with the sequencer.
  * This indicates that we have successfully processed all events up to and including this event.
  * We always acknowledge the current clean timestamp on startup if available to indicate to the sequencer that we are
  * running. The periodic interval is based on the host clock not in sequencer time, however any drift is likely
  * insignificant for the purpose of the sequencer acknowledgements (pruning hourly/daily).
  * Errors are logged at error level - periodic failures are likely not problematic however continuous errors
  * could eventually be problematic for the sequencer operator.
  */
class PeriodicAcknowledgements(
    isHealthy: => Boolean,
    interval: FiniteDuration,
    fetchLatestCleanTimestamp: TraceContext => Future[Option[CantonTimestamp]],
    acknowledge: Traced[CantonTimestamp] => Future[Unit],
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasFlushFuture {
  private val priorAckRef = new AtomicReference[Option[CantonTimestamp]](None)

  private def update(): Unit =
    withNewTraceContext { implicit traceContext =>
      def ackIfChanged(timestamp: CantonTimestamp): Future[Unit] = {
        val priorAck = priorAckRef.getAndSet(Some(timestamp))
        val changed = !priorAck.contains(timestamp)
        if (changed) {
          logger.debug(s"Acknowledging clean timestamp: $timestamp")
          acknowledge(Traced(timestamp))
        } else Future.unit
      }

      if (isHealthy) {
        val updateF = performUnlessClosingF(functionFullName) {
          for {
            latestClean <- fetchLatestCleanTimestamp(traceContext)
            _ <- latestClean.fold(Future.unit)(ackIfChanged)
          } yield ()
        }.onShutdown(
          logger.debug("Acknowledging sequencer timestamp skipped due to shutdown")
        )
        addToFlushAndLogError("periodic acknowledgement")(updateF)
      } else {
        logger.debug("Skipping periodic acknowledgement because sequencer client is not healthy")
      }
    }

  private def scheduleNextUpdate(): Unit = {
    clock
      .scheduleAfter(_ => update(), interval.toJava)
      .map(_ => scheduleNextUpdate())
      .discard[FutureUnlessShutdown[Unit]]
  }

  @VisibleForTesting
  def flush(): Future[Unit] = doFlush()

  // perform one update immediate and then schedule the next
  update()
  scheduleNextUpdate()
}

object PeriodicAcknowledgements {
  type FetchCleanTimestamp = TraceContext => Future[Option[CantonTimestamp]]
  val noAcknowledgements: FetchCleanTimestamp = _ => Future.successful(None)

  def create(
      interval: FiniteDuration,
      isHealthy: => Boolean,
      client: SequencerClient,
      fetchCleanTimestamp: FetchCleanTimestamp,
      clock: Clock,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PeriodicAcknowledgements = {
    new PeriodicAcknowledgements(
      isHealthy,
      interval,
      fetchCleanTimestamp,
      Traced.lift((ts, tc) =>
        client
          .acknowledgeSigned(ts)(tc)
          .valueOr(e => if (!client.isClosing) throw new RuntimeException(e))
      ),
      clock,
      timeouts,
      loggerFactory,
    )
  }

  def fetchCleanCounterFromStore(
      counterTrackerStore: SequencerCounterTrackerStore
  )(implicit executionContext: ExecutionContext): FetchCleanTimestamp =
    traceContext =>
      for {
        cursorO <- counterTrackerStore.preheadSequencerCounter(traceContext)
        timestampO = cursorO.map(_.timestamp)
      } yield timestampO
}

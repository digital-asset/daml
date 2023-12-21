// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.protocol.{
  Deliver,
  DeliverError,
  MessageId,
  SequencedEvent,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.MonadUtil.sequentialTraverse_
import com.google.common.annotations.VisibleForTesting

import java.time.Instant
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

/** When a we make a send request to the sequencer it will not be sequenced until some point in the future and may not
  * be sequenced at all. To track a request call `send` with the messageId and max-sequencing-time of the request,
  * the tracker then observes sequenced events and will notify the provided handler whether the send times out.
  * For aggregatable submission requests, the send tracker notifies the handler of successful sequencing of the submission request,
  * not of successful delivery of the envelopes when the
  * [[com.digitalasset.canton.sequencing.protocol.AggregationRule.threshold]] has been reached.
  * In fact, there is no notification of whether the threshold was reached before the max sequencing time.
  */
class SendTracker(
    initialPendingSends: Map[MessageId, CantonTimestamp],
    store: SendTrackerStore,
    metrics: SequencerClientMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends NamedLogging
    with FlagCloseableAsync
    with AutoCloseable {

  private implicit val directExecutionContext: DirectExecutionContext = DirectExecutionContext(
    noTracingLogger
  )

  /** Details of sends in flight
    * @param startedAt The time the request was made for calculating the elapsed duration for metrics.
    *                  We use the host clock time for this value and it is only tracked ephemerally
    *                  as the elapsed value will not be useful if the local process restarts during sequencing.
    */
  private case class PendingSend(
      maxSequencingTime: CantonTimestamp,
      callback: SendCallback,
      startedAt: Option[Instant],
      traceContext: TraceContext,
  )

  private val pendingSends: TrieMap[MessageId, PendingSend] =
    (TrieMap.newBuilder ++= initialPendingSends map {
      // callbacks and startedAt times will be lost between restarts of the sequencer client
      case (messageId, maxSequencingTime) =>
        messageId -> PendingSend(
          maxSequencingTime,
          SendCallback.empty,
          startedAt = None,
          TraceContext.empty,
        )
    }).result()

  def track(
      messageId: MessageId,
      maxSequencingTime: CantonTimestamp,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SavePendingSendError, Unit] = {
    performUnlessClosing(s"track $messageId") {
      for {
        _ <- store
          .savePendingSend(messageId, maxSequencingTime)
        _ = pendingSends.put(
          messageId,
          PendingSend(maxSequencingTime, callback, startedAt = Some(Instant.now()), traceContext),
        ) match {
          case Some(previousMaxSequencingTime) =>
            // if we were able to persist the new message id without issue but found the message id in our in-memory
            // pending set it suggests either:
            //  - the database has been modified by a writer other than this sequencer client (so its pending set is not in sync)
            //  - there is a bug :-|
            sys.error(
              s"""The SequencerClient pending set of sends is out of sync from the database.
                 |The database reported no send for $messageId but our pending set includes a prior send with mst of $previousMaxSequencingTime.""".stripMargin
            )
          case _none => // we're good
        }
        _ = metrics.submissions.inFlight.inc()
      } yield ()
    }.onShutdown {
      callback(UnlessShutdown.AbortedDueToShutdown)
      EitherT.pure(())
    }
  }

  /** Cancels a pending send without notifying any callers of the result.
    * Should only be used if the send operation itself fails and the transport returns an error
    * indicating that the send will never be sequenced. The SequencerClient should then call cancel
    * to immediately allow retries with the same message-id and then propagate the send error
    * to the caller.
    */
  def cancelPendingSend(messageId: MessageId)(implicit traceContext: TraceContext): Future[Unit] =
    removePendingSendUnlessTimeout(messageId, resultO = None, sequencedTimeO = None)

  /** Provide the latest sequenced events to update the send tracker
    *
    * Callers must not call this concurrently and it is assumed that it is called with sequenced events in order of sequencing.
    * On receiving an event it will perform the following steps in order:
    *   1. If the event is a Deliver or DeliverError from a send that is being tracked it will stop tracking this message id.
    *      This allows using the message-id for new sends.
    *   2. Checks for any pending sends that have a max-sequencing-time that is less than the timestamp of this event.
    *      These events have timed out and a correct sequencer implementation will no longer sequence any events for this send.
    *      The callback of the pending event will be called with the outcome result.
    *
    * The operations performed by update are not atomic, if an error is encountered midway through processing an event
    * then a subsequent replay will cause operations that still have pending sends stored to be retried.
    */
  def update(
      events: Seq[OrdinarySequencedEvent[_]]
  ): Future[Unit] = if (events.isEmpty) Future.unit
  else {
    for {
      maxTimestamp <- events.foldM(CantonTimestamp.MinValue) { case (maxTs, event) =>
        removePendingSend(event.signedEvent.content)(event.traceContext).map { _ =>
          maxTs.max(event.timestamp)
        }
      }
      _ <- processTimeouts(maxTimestamp)
    } yield ()
  }

  private def processTimeouts(
      timestamp: CantonTimestamp
  ): Future[Unit] = {
    val timedOut = pendingSends.collect {
      case (messageId, PendingSend(maxSequencingTime, _, _, traceContext))
          if maxSequencingTime < timestamp =>
        Traced(messageId)(traceContext)
    }.toList
    // parallel would be okay
    sequentialTraverse_(timedOut)(_.withTraceContext { implicit traceContext =>
      handleTimeout(timestamp)
    })
  }

  @VisibleForTesting
  protected def handleTimeout(timestamp: CantonTimestamp)(
      messageId: MessageId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(s"Sequencer send [$messageId] has timed out at $timestamp")
    for {
      _ <- removePendingSendUnlessTimeout(
        messageId,
        UnlessShutdown.Outcome(SendResult.Timeout(timestamp)).some,
        None, // none because the message really timed out
      )
    } yield ()
  }

  private def removePendingSend(
      event: SequencedEvent[_]
  )(implicit traceContext: TraceContext): Future[Unit] =
    extractSendResult(event)
      .fold(Future.unit) { case (messageId, sendResult) =>
        removePendingSendUnlessTimeout(
          messageId,
          UnlessShutdown.Outcome(sendResult).some,
          Some(event.timestamp),
        )
      }

  private def updateSequencedMetrics(pendingSend: PendingSend, result: SendResult): Unit = {
    def recordSequencingTime(): Unit = {
      withEmptyMetricsContext { implicit metricsContext =>
        pendingSend.startedAt foreach { startedAt =>
          val elapsed = java.time.Duration.between(startedAt, Instant.now())
          metrics.submissions.sequencingTime.update(elapsed)
        }
      }
    }

    result match {
      case SendResult.Success(_) => recordSequencingTime()
      case SendResult.Error(_) =>
        // even though it's an error the sequencer still sequenced our request
        recordSequencingTime()
      case SendResult.Timeout(_) =>
        // intentionally not updating sequencing time as this implies no event was sequenced from our request
        metrics.submissions.dropped.inc()
    }
  }

  /** Removes the pending send.
    * If a send result is supplied the callback will be called with it.
    * If the sequencedTime is supplied and it is more recent than the max-sequencing time of the
    * event, then we will not remove the pending send.
    */
  private def removePendingSendUnlessTimeout(
      messageId: MessageId,
      resultO: Option[UnlessShutdown[SendResult]],
      sequencedTimeO: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    // note: this should be okay from a concurrency perspective as there should be only one active
    // send with this message-id at a time (track would fail otherwise)
    val current = pendingSends.get(messageId)
    val removeUnlessTimedOut = pendingSends.updateWith(messageId) {
      case Some(pending) if sequencedTimeO.exists(_ > pending.maxSequencingTime) => Some(pending)
      case other =>
        // this shouldn't happen (as per above),  but let's leave a note in the logs if it does
        if (other != current)
          logger.error(s"Concurrent modification of pending sends $other / $current")
        None
    }
    (removeUnlessTimedOut, current) match {
      // if the sequencedTime is passed and it is more recent than the max-sequencing time of the
      // event, then we will not remove the pending send (it will be picked up later by the handleTimeout method)
      case (None, Some(pending)) =>
        resultO.foreach { result =>
          result.foreach(updateSequencedMetrics(pending, _))
          pending.callback(result)
        }
        for {
          _ <- store.removePendingSend(messageId)
        } yield {
          metrics.submissions.inFlight.dec()
        }
      case (Some(_), _) =>
        // We observed the command being sequenced but it arrived too late to be processed.
        Future.unit
      case _ =>
        logger.debug(s"Removing unknown pending command ${messageId}")
        store.removePendingSend(messageId)
    }
  }

  private def extractSendResult(
      event: SequencedEvent[_]
  )(implicit traceContext: TraceContext): Option[(MessageId, SendResult)] = {
    Option(event) collect {
      case deliver @ Deliver(_, _, _, Some(messageId), _) =>
        logger.trace(s"Send [$messageId] was successful")
        (messageId, SendResult.Success(deliver))

      case error @ DeliverError(_, _, _, messageId, reason) =>
        logger.debug(s"Send [$messageId] failed: $reason")
        (messageId, SendResult.Error(error))
    }
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    Seq(
      AsyncCloseable(
        "complete-pending-sends",
        MonadUtil.sequentialTraverse_(pendingSends.keys)(
          removePendingSendUnlessTimeout(_, Some(UnlessShutdown.AbortedDueToShutdown), None)
        ),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable("send-tracker-store", store.close()),
    )
  }
}

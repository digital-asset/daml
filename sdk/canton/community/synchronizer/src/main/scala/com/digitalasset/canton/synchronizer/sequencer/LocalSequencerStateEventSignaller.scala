// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{LoggerUtil, PekkoUtil}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** If all Sequencer writes are occurring locally we pipe write notifications to read subscriptions
  * allowing the [[SequencerReader]] to immediately read from the backing store rather than polling.
  *
  * An important caveat is that we only supply signals when a write for a member occurs. If there
  * are no writes from starting the process the member will never receive a read signal. The
  * [[SequencerReader]] is responsible for performing at least one initial read from the store to
  * ensure that all prior events are served as required.
  *
  * Not suitable or at least very sub-optimal for a horizontally scaled sequencer setup where a
  * reader will not have visibility of all writes locally.
  */
class LocalSequencerStateEventSignaller(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends EventSignaller
    with FlagCloseableAsync
    with NamedLogging {

  private val (queue, notificationsHubSource) = {
    implicit val traceContext: TraceContext = TraceContext.empty
    PekkoUtil.runSupervised(
      Source
        .queue[Traced[WriteNotification]](1, OverflowStrategy.backpressure)
        // this conflate kicks in, when there is no downstream consumer, to not exert backpressure to the upstream producer
        .conflate((left, right) =>
          Traced(left.value.union(right.value))(right.traceContext)
        ) // keep the trace context of the latest notification
        .toMat(BroadcastHub.sink(1))(Keep.both),
      errorLogMessagePrefix = "LocalStateEventSignaller flow failed",
    )
  }

  private val notificationsHubSourceWithLogging =
    notificationsHubSource.map { tracedNotification =>
      tracedNotification.withTraceContext { implicit traceContext => notification =>
        // TODO(#26818): clean up excessive debug logging
        logger.debug(
          s"Broadcasting notification: $notification"
        )
      }
      tracedNotification
    }

  override def notifyOfLocalWrite(
      notification: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] =
    performUnlessClosingF(functionFullName) {
      queueWithLogging("latest-head-state-queue", queue)(notification)
    }.onShutdown {
      logger.info("Dropping local write signal due to shutdown")
      // Readers/subscriptions should be shut down separately
    }

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  )(implicit traceContext: TraceContext): Source[Traced[ReadSignal], NotUsed] = {
    logger.info(
      s"Creating signal source for $member (id: $memberId)"
    ) // TODO(i28037): remove extra logging
    notificationsHubSourceWithLogging
      .filter(_.value.includes(memberId))
      .map { notification =>
        // TODO(#26818): clean up excessive debug logging
        logger.debug(s"Processing read signal for $member due to $notification")(
          notification.traceContext
        )
        notification.map(_ => ReadSignal)
      }
      // this conflate ensures that a slow consumer doesn't cause backpressure and therefore
      // block the stream of signals for other consumers
      .conflate((_, right) => right)
      .map { tracedReadSignal =>
        // TODO(#26818): clean up excessive debug logging
        logger.debug(s"Emitting read signal for $member")(tracedReadSignal.traceContext)
        tracedReadSignal
      }
  }

  private def queueWithLogging(
      name: String,
      queue: SourceQueueWithComplete[Traced[WriteNotification]],
  )(
      item: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val logLevel = item match {
      case WriteNotification.None => Level.TRACE
      case _: WriteNotification.Members => Level.DEBUG
    }
    LoggerUtil.logAtLevel(logLevel, s"Pushing item to $name: $item")
    queue
      .offer(Traced(item)(traceContext))
      .transform { // TODO(i28037): remove extra logging, use map instead of transform
        case Success(result: QueueCompletionResult) =>
          logger.warn(s"Failed to queue item on $name: $result")
          Success(())
        case Success(QueueOfferResult.Enqueued) =>
          // TODO(#26818): clean up excessive debug logging
          logger.debug("Push successful.")
          Success(())
        case Success(QueueOfferResult.Dropped) =>
          logger.warn(s"Dropped item while trying to queue on $name")
          Success(())
        case Failure(ex) =>
          logger.warn(s"Pushing item failed with an exception", ex)
          Failure(ex)
      }
  }

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*

    Seq(
      SyncCloseable("queue.complete", queue.complete()),
      AsyncCloseable(
        "queue.watchCompletion",
        queue.watchCompletion(),
        timeouts.shutdownShort,
      ),
      // `watchCompletion` completes when the queue's contents have been consumed by the `conflate`,
      // but `conflate` need not yet have passed the conflated element to the BroadcastHub.
      // So we create a new subscription and wait until the completion signal has propagated.
      AsyncCloseable(
        "queue.completion",
        notificationsHubSource.runWith(Sink.ignore),
        timeouts.shutdownShort,
      ),
      // Other readers of the broadcast hub should be shut down separately
    )
  }
}

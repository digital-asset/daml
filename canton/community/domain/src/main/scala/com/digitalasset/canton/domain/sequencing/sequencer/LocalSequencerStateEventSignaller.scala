// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{PekkoUtil, LoggerUtil}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

/** If all Sequencer writes are occurring locally we pipe write notifications to read subscriptions allowing the
  * [[SequencerReader]] to immediately read from the backing store rather than polling.
  *
  * An important caveat is that we only supply signals when a write for a member occurs. If there are no writes from
  * starting the process the member will never receive a read signal. The [[SequencerReader]] is responsible for
  * performing at least one initial read from the store to ensure that all prior events are served as required.
  *
  * Not suitable or at least very sub-optimal for a horizontally scaled sequencer setup where a reader will not have
  * visibility of all writes locally.
  */
class LocalSequencerStateEventSignaller(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends EventSignaller
    with FlagCloseableAsync
    with NamedLogging {

  private val (queue, notificationsHubSource) =
    PekkoUtil.runSupervised(
      logger.error("LocalStateEventSignaller flow failed", _)(TraceContext.empty),
      Source
        .queue[WriteNotification](1, OverflowStrategy.backpressure)
        .conflate(_ union _)
        .toMat(BroadcastHub.sink(1))(Keep.both),
    )

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
  )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] = {
    logger.debug(s"Creating signal soruce for $member")
    notificationsHubSource
      .filter(_.includes(memberId))
      .map(_ => ReadSignal)
  }

  private def queueWithLogging(name: String, queue: SourceQueueWithComplete[WriteNotification])(
      item: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val logLevel = item match {
      case WriteNotification.None => Level.TRACE
      case _: WriteNotification.Members => Level.DEBUG
    }
    LoggerUtil.logAtLevel(logLevel, s"Pushing item to $name: $item")
    queue.offer(item) map {
      case result: QueueCompletionResult =>
        logger.warn(s"Failed to queue item on $name: $result")
      case QueueOfferResult.Enqueued =>
      case QueueOfferResult.Dropped =>
        logger.warn(s"Dropped item while trying to queue on $name")
    }
  }

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*

    Seq(
      SyncCloseable("queue.complete", queue.complete()),
      AsyncCloseable(
        "queue.watchCompletion",
        queue.watchCompletion(),
        timeouts.shutdownShort.unwrap,
      ),
      // `watchCompletion` completes when the queue's contents have been consumed by the `conflate`,
      // but `conflate` need not yet have passed the conflated element to the BroadcastHub.
      // So we create a new subscription and wait until the completion signal has propagated.
      AsyncCloseable(
        "queue.completion",
        notificationsHubSource.runWith(Sink.ignore),
        timeouts.shutdownShort.unwrap,
      ),
      // Other readers of the broadcast hub should be shut down separately
    )
  }
}

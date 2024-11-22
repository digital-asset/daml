// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{ParticipantUpdate, Update}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.platform.indexer.IndexerState.RepairInProgress
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil, SimpleExecutionQueue}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

/** Helper to publish participant events in a thread-safe way. For "regular" domain related events
  * thread safety is taken care of by the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]].
  *
  * ParticipantEventPublisher also encapsulates the participant clock generating unique participant recordTime.
  *
  * @param participantId        participant id
  * @param participantClock     clock for the current time to stamp published events with
  * @param loggerFactory        named logger factory
  */
class ParticipantEventPublisher(
    participantId: ParticipantId,
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    participantClock: Clock,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import com.digitalasset.canton.util.ShowUtil.*

  private val executionQueue = new SimpleExecutionQueue(
    "participant-event-publisher-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private def publishInternal(event: Update)(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(s"Publishing event at record time ${event.recordTime}: ${event.show}")
    ledgerApiIndexer.value.queue
      .offer(event)
      // waiting for the participant event to persisted by Indexer
      .flatMap(_ => event.persisted.future)
  }

  private def retryIfRepairInProgress(
      retryBeforeWarning: Int = 10
  )(f: => Future[Unit])(implicit traceContext: TraceContext): Future[Unit] =
    f.recoverWith {
      case repairInProgress: RepairInProgress =>
        val logLevel =
          if (retryBeforeWarning <= 0) Level.WARN
          else Level.INFO
        LoggerUtil.logAtLevel(
          logLevel,
          "Delaying the publication of participant event, as Repair operation is in progress. Waiting until Repair operation finished...",
        )
        repairInProgress.repairDone.flatMap { _ =>
          LoggerUtil.logAtLevel(
            logLevel,
            "Repair operation finished...try to publish participant event again.",
          )
          retryIfRepairInProgress(retryBeforeWarning - 1)(f)
        }

      case t =>
        Future.failed(t)
    }

  /** Events published this way will be delayed by ongoing repair-operations, meaning:
    * the publication of this event will be postponed until the repair-operation is finished.
    * Repair operations take precedence: after 10 times waiting for the ongoing
    * repair to finish, a warning will be emitted for each subsequent retry.
    *
    * @return A Future which will be only successful if the event is successfully persisted by the indexer
    */
  def publishEventDelayableByRepairOperation(
      event: ParticipantUpdate
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    ErrorUtil.requireArgument(
      event.recordTime == ParticipantEventPublisher.now,
      show"RecordTime not initialized with 'now' literal. Participant event: $event",
    )
    executionQueue.execute(
      // this is hopefully a temporary measure, and is needed since we schedule party notifications upon the related topology change's effective time, so we can never be sure, even if the domain is already disconnected that the participant event publisher is not used
      // in case that is not needed anymore (thanks to ledger api party and topology unification),
      // we should be able to plainly fail the rest of the events
      retryIfRepairInProgress()(
        publishInternal(event.withRecordTime(participantClock.uniqueTime()))
      ),
      s"publish event ${event.show} with record time ${event.recordTime}",
    )
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)
}

object ParticipantEventPublisher {
  // Used to indicate that timestamp is supposed to be overwritten with participant clock time. Only acceptable
  // recordTime for participant events to publish.
  val now: CantonTimestamp = CantonTimestamp.Epoch
}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.error.utils.DecodedCantonError
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  InFlightSubmissionTrackerError,
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
  UnsequencedSubmissionMap,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.AggregateSubmissionAlreadySent
import com.digitalasset.canton.sequencing.protocol.{DeliverError, MessageId}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, blocking}

/** Tracker for in-flight submissions backed by the
  * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  *
  * A submission is in-flight if it is in the
  * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]]. The tracker registers a
  * submission before the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]] is sent
  * to the [[com.digitalasset.canton.sequencing.client.SequencerClient]] of a synchronizer. After
  * the corresponding event has been published to the indexer, the submission will be removed from
  * the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] again. This happens
  * normally as part of request processing after phase 7. If the submission has not been sequenced
  * by the specified
  * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]], say
  * because the submission was lost on the way to the sequencer, the participant generates an
  * appropriate update because the submission will never reach request processing. The latter must
  * work even if the participant crashes (if run with persistence).
  */
class InFlightSubmissionTracker(
    store: Eval[InFlightSubmissionStore],
    deduplicator: CommandDeduplicator,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def processPublications(
      publications: Seq[PostPublishData]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- deduplicator.processPublications(publications)
      trackedReferences = publications.map(publication =>
        publication.publishSource match {
          case PublishSource.Local(messageUuid) =>
            InFlightByMessageId(
              synchronizerId = publication.submissionSynchronizerId,
              messageId = MessageId.fromUuid(messageUuid),
            )

          case PublishSource.Sequencer(requestSequencerCounter, sequencerTimestamp) =>
            InFlightBySequencingInfo(
              synchronizerId = publication.submissionSynchronizerId,
              sequenced = SequencedSubmission(
                sequencerCounter = requestSequencerCounter,
                sequencingTime = sequencerTimestamp,
              ),
            )
        }
      )
      _ <- store.value
        .delete(trackedReferences)
    } yield ()

  /** Create and initialize an InFlightSubmissionSynchronizerTracker for synchronizerId.
    *
    * Steps of initialization: Deletes the published, sequenced in-flight submissions with
    * sequencing timestamps up to the given bound and informs the [[CommandDeduplicator]] about the
    * published events. Prepares the unsequencedSubmissionMap with all the in-flight, unsequenced
    * entries and schedules for them potential publication at the retrieved timeout (or immediately
    * if already in the past).
    */
  def inFlightSubmissionSynchronizerTracker(
      synchronizerId: SynchronizerId,
      recordOrderPublisher: RecordOrderPublisher,
      timeTracker: SynchronizerTimeTracker,
      metrics: ConnectedSynchronizerMetrics,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[InFlightSubmissionSynchronizerTracker] = {
    val unsequencedSubmissionMap: UnsequencedSubmissionMap[SubmissionTrackingData] =
      new UnsequencedSubmissionMap(
        synchronizerId = synchronizerId,
        sizeWarnThreshold = 1000000,
        unsequencedInFlightGauge =
          metrics.inFlightSubmissionSynchronizerTracker.unsequencedInFlight,
        loggerFactory = loggerFactory,
      )
    def pullTimelyRejectEvent(
        messageId: MessageId,
        recordTime: CantonTimestamp,
    ): Option[Update] =
      unsequencedSubmissionMap
        .pull(messageId)
        .map { entry =>
          implicit val traceContext: TraceContext = entry.traceContext
          entry.trackingData.rejectionEvent(recordTime, entry.messageUuid)
        }

    for {
      unsequencedInFlights <- store.value.lookupUnsequencedUptoUnordered(
        synchronizerId,
        CantonTimestamp.MaxValue,
      )
      // Re-request ticks for all remaining unsequenced timestamps
      _ = if (unsequencedInFlights.nonEmpty) {
        timeTracker.requestTicks(
          unsequencedInFlights.map(_.sequencingInfo.timeout)
        )
      }
      // Recover internal state: store unsequencedInFlights in unsequencedSubmissionMap, and schedule the rejection
      _ = unsequencedInFlights.foreach { unsequencedInFlight =>
        val submissionTraceContext = unsequencedInFlight.submissionTraceContext
        unsequencedSubmissionMap.pushIfNotExists(
          unsequencedInFlight.messageUuid,
          unsequencedInFlight.sequencingInfo.trackingData,
          submissionTraceContext,
          unsequencedInFlight.rootHashO,
        )
        recordOrderPublisher
          .scheduleFloatingEventPublication( // first try to schedule with the recovered timeout
            timestamp = unsequencedInFlight.sequencingInfo.timeout,
            eventFactory = pullTimelyRejectEvent(unsequencedInFlight.messageId, _),
          )(submissionTraceContext)
          .valueOr { ropIsAlreadyAt =>
            logger.debug(
              s"Unsequenced Inflight Submission's sequencing timeout ${unsequencedInFlight.sequencingInfo.timeout} is expired: record time is already at $ropIsAlreadyAt, scheduling timely rejection as soon as possible at synchronizer startup. [message ID: ${unsequencedInFlight.messageId}]"
            )
            recordOrderPublisher
              .scheduleFloatingEventPublicationImmediately( // if the first try fails (timeout already expired), scheduling immediately
                eventFactory = pullTimelyRejectEvent(unsequencedInFlight.messageId, _)
              )(submissionTraceContext)
              .discard
          }
      }
    } yield new InFlightSubmissionSynchronizerTracker(
      synchronizerId = synchronizerId,
      store = store,
      deduplicator = deduplicator,
      recordOrderPublisher = recordOrderPublisher,
      timeTracker = timeTracker,
      unsequencedSubmissionMap = unsequencedSubmissionMap,
      loggerFactory = loggerFactory,
    )
  }
}

class InFlightSubmissionSynchronizerTracker(
    synchronizerId: SynchronizerId,
    store: Eval[InFlightSubmissionStore],
    deduplicator: CommandDeduplicator,
    recordOrderPublisher: RecordOrderPublisher,
    timeTracker: SynchronizerTimeTracker,
    unsequencedSubmissionMap: UnsequencedSubmissionMap[SubmissionTrackingData],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  /** Registers the given submission as being in flight and unsequenced unless there already is an
    * in-flight submission for the same change ID or the timeout has already elapsed. It is expected
    * that client always calls this function with a unique messageUuid
    *
    * @return
    *   The actual deduplication offset that is being used for deduplication for this submission
    */
  def register(
      submission: InFlightSubmission[UnsequencedSubmission],
      deduplicationPeriod: DeduplicationPeriod,
  ): EitherT[FutureUnlessShutdown, InFlightSubmissionTrackerError, Either[
    DeduplicationFailed,
    DeduplicationPeriod.DeduplicationOffset,
  ]] = {
    implicit val traceContext: TraceContext = submission.submissionTraceContext
    for {
      _ <- recordOrderPublisher.scheduleFloatingEventPublication(
        timestamp = submission.sequencingInfo.timeout,
        eventFactory = pullTimelyRejectEvent(submission.messageId, _),
        onScheduled = { () =>
          store.value
            .register(submission)
            .map(_ =>
              // if the submission successfully registered, we also add the entry in the unsequencedSubmissionMap
              // - without this entry nothing will happen if the scheduled task is executing, as pullTimelyRejectEvent
              //   will be empty
              // - we are waiting for this to happen before executing the scheduled task, because onScheduled will complete
              //   after this, and scheduleFloatingEventPublication ensures that the onScheduled task is waited for
              //   before executing.
              //   This is important to not have a race between the persistent .register and the publishing of the timeout
              //   CommandRejected event - which at post-processing will try to remove the registered event from persistence
              unsequencedSubmissionMap.pushIfNotExists(
                submission.messageUuid,
                submission.sequencingInfo.trackingData,
                submission.submissionTraceContext,
                None,
              )
            )
            .value
        },
      ) match {
        case Left(markTooLow) =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            TimeoutTooLow(submission, markTooLow): InFlightSubmissionTrackerError
          )

        case Right(eitherTValue) =>
          EitherT(eitherTValue)
            .leftMap(SubmissionAlreadyInFlight(submission, _))
            .leftWiden[InFlightSubmissionTrackerError]
      }
      // It is safe to request a tick only after persisting the in-flight submission
      // because if we crash in between, crash recovery will request the tick.
      _ = timeTracker.requestTick(submission.sequencingInfo.timeout)
      // After the registration of the in-flight submission, we want to deduplicate the command.
      // A command deduplication failure must be reported via a completion event
      // because we do not know whether we have already produced a timely rejection concurrently.
      deduplicationResult <- EitherT
        .right(deduplicator.checkDuplication(submission.changeIdHash, deduplicationPeriod).value)
    } yield deduplicationResult
  }

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.updateRegistration */
  def updateRegistration(
      submission: InFlightSubmission[UnsequencedSubmission],
      rootHash: RootHash,
  ): FutureUnlessShutdown[Unit] = {
    implicit val traceContext: TraceContext = submission.submissionTraceContext
    store.value
      .updateRegistration(submission, rootHash)
      .map(_ =>
        unsequencedSubmissionMap.addRootHashIfNotSpecifiedYet(
          submission.messageId,
          rootHash,
        )
      )
  }

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.observeSequencing */
  def observeSequencing(sequenceds: Map[MessageId, SequencedSubmission])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    store.value
      .observeSequencing(synchronizerId, sequenceds)
      .map(_ =>
        sequenceds.keysIterator
          .foreach(unsequencedSubmissionMap.pull(_).discard)
      )
  // nothing else to do: the scheduled task will execute in ROP, and will produce no event

  /** @see
    *   com.digitalasset.canton.participant.store.InFlightSubmissionStore.observeSequencedRootHash
    */
  def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    store.value
      .observeSequencedRootHash(rootHash, submission)
      .map(_ => unsequencedSubmissionMap.pullByHash(rootHash))

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.updateUnsequenced */
  def observeSubmissionError(
      changeIdHash: ChangeIdHash,
      messageId: MessageId,
      newTrackingData: SubmissionTrackingData,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.value
      .updateUnsequenced(
        changeIdHash,
        synchronizerId,
        messageId,
        UnsequencedSubmission(
          // This timeout has relevance only for crash recovery (will be used there to publish the rejection).
          // We try to approximate it as precision is not paramount (this approximation is likely too low, resulting in an immediate delivery on crash recovery).
          // In theory we could pipe the realized immediate-synchronizer time back to the persistence, but then we would need to wait for persisting before letting
          // the synchronizer go, which would have the undesirable effect of submission errors are slowing down the synchronizer.
          // Please note: as this data is also used in a heuristic for journal garbage collection, we must use here realistic values.

          // first approximation is by synchronizer-time-tracker
          timeTracker.latestTime
            .getOrElse(
              // if no synchronizer-time yet, then approximating with the initial time of the synchronizer
              recordOrderPublisher.initTimestamp
            ),
          newTrackingData,
        ),
      )
      .map { _ =>
        unsequencedSubmissionMap.changeIfExists(
          key = messageId,
          trackingData = newTrackingData,
        )
        recordOrderPublisher
          .scheduleFloatingEventPublicationImmediately(
            eventFactory = pullTimelyRejectEvent(messageId, _)
          )
          .discard
      }

  /** Updates the unsequenced submission corresponding to the
    * [[com.digitalasset.canton.sequencing.protocol.DeliverError]], if any, using
    * [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.updateOnNotSequenced]].
    */
  def observeDeliverError(
      deliverError: DeliverError
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def updatedTrackingData(
        inFlightO: Option[InFlightSubmission[SubmissionSequencingInfo]]
    ): Option[(ChangeIdHash, UnsequencedSubmission, TraceContext)] =
      for {
        inFlight <- inFlightO
        unsequencedO = inFlight.sequencingInfo.asUnsequenced
        _ = if (unsequencedO.isEmpty) {
          logger.warn(
            s"Received a deliver error for the sequenced submission $inFlight. Deliver error $deliverError"
          )
        }
        unsequenced <- unsequencedO
        newTrackingData <- unsequenced.trackingData.updateOnNotSequenced(
          deliverError.timestamp,
          deliverError.reason,
        )
      } yield (inFlight.changeIdHash, newTrackingData, inFlight.submissionTraceContext)

    val messageId = deliverError.messageId

    // Ignore already sequenced errors here to deal with submission request amplification
    val isAlreadySequencedError = DecodedCantonError
      .fromGrpcStatus(deliverError.reason)
      .exists(_.code.id == AggregateSubmissionAlreadySent.id)
    if (isAlreadySequencedError) {
      logger.debug(
        s"Ignoring deliver error $deliverError for $synchronizerId and $messageId because the message was already sequenced."
      )
      FutureUnlessShutdown.unit
    } else
      for {
        inFlightO <- store.value.lookupSomeMessageId(synchronizerId, messageId)
        toUpdateO = updatedTrackingData(inFlightO)
        _ <- toUpdateO.traverse_ { case (changeIdHash, newTrackingData, submissionTraceContext) =>
          store.value
            .updateUnsequenced(changeIdHash, synchronizerId, messageId, newTrackingData)
            .map { _ =>
              unsequencedSubmissionMap.changeIfExists(
                key = messageId,
                trackingData = newTrackingData.trackingData,
              )
              recordOrderPublisher
                .scheduleFloatingEventPublication(
                  timestamp =
                    newTrackingData.timeout, // this is the delivery error's timeout, we publish before ticking this sequencer counter + timestamp (no need to request a tick)
                  eventFactory = pullTimelyRejectEvent(messageId, _),
                )(submissionTraceContext)
                .leftMap(ropIsAlreadyAt =>
                  throw new IllegalStateException(
                    s"RecordOrderPublisher is already at $ropIsAlreadyAt, cannot schedule rejection event (at ${newTrackingData.timeout})"
                  )
                )
                .merge
            }
        }
      } yield ()
  }

  private def pullTimelyRejectEvent(
      messageId: MessageId,
      recordTime: CantonTimestamp,
  ): Option[Update] =
    unsequencedSubmissionMap
      .pull(messageId)
      .map { entry =>
        implicit val traceContext: TraceContext = entry.traceContext
        entry.trackingData.rejectionEvent(recordTime, entry.messageUuid)
      }
}

object InFlightSubmissionTracker {

  sealed trait InFlightSubmissionTrackerError extends Product with Serializable

  final case class SubmissionAlreadyInFlight(
      newSubmission: InFlightSubmission[UnsequencedSubmission],
      existingSubmission: InFlightSubmission[SubmissionSequencingInfo],
  ) extends InFlightSubmissionTrackerError

  final case class TimeoutTooLow(
      submission: InFlightSubmission[UnsequencedSubmission],
      lowerBound: CantonTimestamp,
  ) extends InFlightSubmissionTrackerError

  /** For in-memory tracking the unsequenced submissions. If submission is sequenced, the entry is
    * removed. If submission is rejected before sequencing, the entry will be removed and a
    * corresponding floating rejection update will be published. On crash recovery the internal
    * state of this map needs to be recovered from persistence, which should be preceded by
    * post-publish part of the crash recovery (getting the InFlightSubmissionStore uptodate with the
    * ledger-end)
    *
    * @param sizeWarnThreshold
    *   defines an upper size-limit for the in-memory storage which above WARN messages will be
    *   emitted
    * @param unsequencedInFlightGauge
    *   for tracking the size of the in-memory storage
    */
  final class UnsequencedSubmissionMap[T](
      synchronizerId: SynchronizerId,
      sizeWarnThreshold: Int,
      unsequencedInFlightGauge: Gauge[Int],
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    private val mutableUnsequencedMap: mutable.Map[MessageId, Entry[T]] =
      mutable.Map()
    private val rootHashMap: mutable.Map[RootHash, MessageId] =
      mutable.Map()

    // only change if exists, if not exists do nothing
    def changeIfExists(
        key: MessageId,
        trackingData: T,
    ): Unit = blocking(
      synchronized(
        mutableUnsequencedMap
          .updateWith(key) {
            case Some(entry) =>
              Some(entry.copy(trackingData = trackingData))

            case None => None
          }
          .discard
      )
    )

    // only change if exists, and hash is empty, if not exist or hash is defined do nothing
    def addRootHashIfNotSpecifiedYet(
        key: MessageId,
        rootHash: RootHash,
    ): Unit = blocking(
      synchronized(
        mutableUnsequencedMap
          .updateWith(key) {
            case Some(entry @ Entry(_, _, _, None)) =>
              rootHashMap += rootHash -> key
              Some(entry.copy(rootHashO = Some(rootHash)))

            case noChange => noChange
          }
          .discard
      )
    )

    // only add if not exists, if exists do nothing
    def pushIfNotExists(
        messageUuid: UUID,
        trackingData: T,
        submissionTraceContext: TraceContext,
        rootHash: Option[RootHash],
    )(implicit traceContext: TraceContext): Unit = blocking(synchronized {
      val messageId = MessageId.fromUuid(messageUuid)
      if (!mutableUnsequencedMap.contains(MessageId.fromUuid(messageUuid))) {
        mutableUnsequencedMap += messageId -> Entry(
          trackingData,
          messageUuid,
          submissionTraceContext,
          rootHash,
        )
        rootHash.map(_ -> messageId).foreach(rootHashMap += _)
        unsequencedInFlightGauge.updateValue(mutableUnsequencedMap.size)
        if (mutableUnsequencedMap.sizeIs > sizeWarnThreshold)
          logger.warn(
            s"UnsequencedSubmissionMap for synchronizer $synchronizerId is growing too large (threshold with $sizeWarnThreshold breached: ${mutableUnsequencedMap.size})"
          )
      }
    })

    // Only the first call returns an entry, further calls result in None (meaning: already pulled).
    // This is needed as corresponding scheduled tasks are not getting removed, but rather those task will do nothing on perform()
    // if an earlier task already emitted the corresponding event.
    def pull(key: MessageId): Option[Entry[T]] =
      blocking(
        synchronized(
          mutableUnsequencedMap
            .remove(key)
            .map { result =>
              result.rootHashO.foreach(rootHashMap.remove)
              unsequencedInFlightGauge.updateValue(mutableUnsequencedMap.size)
              result
            }
        )
      )

    def pullByHash(rootHash: RootHash): Unit =
      blocking(
        synchronized(
          rootHashMap
            .get(rootHash)
            .foreach(pull)
        )
      )
  }

  final case class Entry[T](
      trackingData: T,
      messageUuid: UUID,
      traceContext: TraceContext,
      rootHashO: Option[RootHash],
  )

}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.daml.error.utils.DecodedCantonError
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
}
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.AggregateSubmissionAlreadySent
import com.digitalasset.canton.sequencing.protocol.{DeliverError, MessageId}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, SingleUseCell}

import scala.concurrent.{ExecutionContext, Future}

/** Tracker for in-flight submissions backed by the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  *
  * A submission is in-flight if it is in the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  * The tracker registers a submission
  * before the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
  * is sent to the [[com.digitalasset.canton.sequencing.client.SequencerClient]] of a domain.
  * After the corresponding event has been published to the indexer,
  * the submission will be removed from the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] again.
  * This happens normally as part of request processing after phase 7.
  * If the submission has not been sequenced by the specified
  * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]],
  * say because the submission was lost on the way to the sequencer,
  * the participant generates an appropriate update because the submission will never reach request processing.
  * The latter must work even if the participant crashes (if run with persistence).
  */
class InFlightSubmissionTracker(
    store: Eval[InFlightSubmissionStore],
    deduplicator: CommandDeduplicator,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {
  import InFlightSubmissionTracker.*

  private val domainStateLookupCell
      : SingleUseCell[DomainId => Option[InFlightSubmissionTrackerDomainState]] =
    new SingleUseCell

  private def domainStates(domainId: DomainId): Option[InFlightSubmissionTrackerDomainState] =
    domainStateLookupCell.get.getOrElse(throw new IllegalStateException)(domainId)

  def registerDomainStateLookup(
      domainStates: DomainId => Option[InFlightSubmissionTrackerDomainState]
  ): Unit =
    domainStateLookupCell
      .putIfAbsent(domainStates)
      .foreach(_ => throw new IllegalStateException("RegisterDomainStateLookup already defined"))

  /** Registers the given submission as being in flight and unsequenced
    * unless there already is an in-flight submission for the same change ID
    * or the timeout has already elapsed.
    *
    * @return The actual deduplication offset that is being used for deduplication for this submission
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
      domainState <- domainStateFor(submission.submissionDomain).mapK(FutureUnlessShutdown.outcomeK)
      _result <- domainState.observedTimestampTracker
        .runIfAboveWatermark(
          submission.sequencingInfo.timeout,
          store.value.register(submission),
        ) match {
        case Left(markTooLow) =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            TimeoutTooLow(submission, markTooLow.highWatermark): InFlightSubmissionTrackerError
          )
        case Right(eitherT) =>
          eitherT
            .leftMap(SubmissionAlreadyInFlight(submission, _))
            .leftWiden[InFlightSubmissionTrackerError]
      }
      // It is safe to request a tick only after persisting the in-flight submission
      // because if we crash in between, crash recovery will request the tick.
      _ = domainState.domainTimeTracker.requestTick(submission.sequencingInfo.timeout)
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

    store.value.updateRegistration(submission, rootHash)
  }

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.observeSequencing */
  def observeSequencing(domainId: DomainId, sequenceds: Map[MessageId, SequencedSubmission])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    store.value.observeSequencing(domainId, sequenceds)

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.observeSequencedRootHash */
  def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    store.value.observeSequencedRootHash(rootHash, submission)

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.updateUnsequenced */
  def observeSubmissionError(
      changeIdHash: ChangeIdHash,
      domainId: DomainId,
      messageId: MessageId,
      newTrackingData: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.value.updateUnsequenced(changeIdHash, domainId, messageId, newTrackingData).map {
      (_: Unit) =>
        // Request a tick for the new timestamp if we're still connected to the domain
        domainStates(domainId) match {
          case Some(domainState) =>
            domainState.domainTimeTracker.requestTick(newTrackingData.timeout)
          case None =>
            logger.debug(
              s"Skipping to request tick at ${newTrackingData.timeout} on $domainId as the domain is not available"
            )
        }
    }

  /** Updates the unsequenced submission corresponding to the [[com.digitalasset.canton.sequencing.protocol.DeliverError]],
    * if any, using [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.updateOnNotSequenced]].
    */
  def observeDeliverError(
      deliverError: DeliverError
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def updatedTrackingData(
        inFlightO: Option[InFlightSubmission[SubmissionSequencingInfo]]
    ): Option[(ChangeIdHash, UnsequencedSubmission)] =
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
      } yield (inFlight.changeIdHash, newTrackingData)

    val domainId = deliverError.domainId
    val messageId = deliverError.messageId

    // Ignore already sequenced errors here to deal with submission request amplification
    val isAlreadySequencedError = DecodedCantonError
      .fromGrpcStatus(deliverError.reason)
      .exists(_.code.id == AggregateSubmissionAlreadySent.id)
    if (isAlreadySequencedError) {
      logger.debug(
        s"Ignoring deliver error $deliverError for $domainId and $messageId because the message was already sequenced."
      )
      FutureUnlessShutdown.unit
    } else
      for {
        inFlightO <- store.value.lookupSomeMessageId(domainId, messageId)
        toUpdateO = updatedTrackingData(inFlightO)
        _ <- toUpdateO.traverse_ { case (changeIdHash, newTrackingData) =>
          store.value.updateUnsequenced(changeIdHash, domainId, messageId, newTrackingData)
        }
      } yield ()
  }

  /** Marks the timestamp as having been observed on the domain. */
  // We could timely reject up to the `timestamp` here,
  // but we would have to implement our own batching.
  // Instead, we piggy-back on when the clean sequencer counter is advanced.
  def observeTimestamp(
      domainId: DomainId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, UnknownDomain, Unit] =
    domainStateFor(domainId).semiflatMap { domainState =>
      domainState.observedTimestampTracker.increaseWatermark(timestamp)
    }

  /** Publishes the rejection events for all unsequenced submissions on `domainId` up to the given timestamp.
    * Does not remove the submissions from the in-flight table as this will happen by the
    * [[processPublications]] called by the indexer towards the end of the processing pipeline (post processing stage).
    */
  def timelyReject(
      domainId: DomainId,
      upToInclusive: CantonTimestamp,
      participantEventPublisher: ParticipantEventPublisher,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownDomain, Unit] =
    performUnlessClosingEitherUSF(functionFullName) {
      domainStateFor(domainId).mapK(FutureUnlessShutdown.outcomeK).semiflatMap { domainState =>
        for {
          // Increase the watermark for two reasons:
          // 1. Below, we publish an event via the ParticipantEventPublisher. This will then call the
          //    processPublications, which deletes the entry from the store. So we must make sure that the deletion
          //    cannot interfere with a concurrent insertion.
          // 2. Timestamps are also observed via the RecordOrderPublisher. If the RecordOrderPublisher
          //    does not advance due to a long-running request-response-result cycle, we get here
          //    a second chance of observing the timestamp when the sequencer counter becomes clean.
          _ <- FutureUnlessShutdown
            .outcomeF(domainState.observedTimestampTracker.increaseWatermark(upToInclusive))
          reject <- doTimelyReject(domainId, upToInclusive, participantEventPublisher)
        } yield reject
      }
    }

  /** Same as [[timelyReject]] except that this method may only be called if [[timelyReject]] has already been called
    * previously with the same or a later timestamp for the same domain.
    */
  def timelyRejectAgain(
      domainId: DomainId,
      upToInclusive: CantonTimestamp,
      participantEventPublisher: ParticipantEventPublisher,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    // No need to bump the watermark like `timelyReject` does because it had been bumped previously.
    // This method therefore can run even if the domain is not connected.
    //
    // Sanity check that the watermark is sufficiently high. If the domain is not available, we skip the sanity check.
    domainStates(domainId).foreach { state =>
      val highWaterMark = state.observedTimestampTracker.highWatermark
      ErrorUtil.requireState(
        upToInclusive <= highWaterMark,
        s"Bound $upToInclusive is above high watermark $highWaterMark despite this being a re-notification",
      )
    }

    doTimelyReject(domainId, upToInclusive, participantEventPublisher)
  }

  private def doTimelyReject(
      domainId: DomainId,
      upToInclusive: CantonTimestamp,
      participantEventPublisher: ParticipantEventPublisher,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    for {
      timelyRejects <- store.value.lookupUnsequencedUptoUnordered(domainId, upToInclusive)
      events = timelyRejects.map(timelyRejectionEventFor)
      _skippedE <- participantEventPublisher.publishDomainRelatedEvents(events)
    } yield ()

  private[this] def timelyRejectionEventFor(
      inFlight: InFlightSubmission[UnsequencedSubmission]
  ): Update = {
    implicit val traceContext: TraceContext = inFlight.submissionTraceContext
    // Use the trace context from the submission for the rejection
    // because we don't have any other later trace context available
    inFlight.sequencingInfo.trackingData
      .rejectionEvent(inFlight.associatedTimestamp, inFlight.messageUuid)
  }

  def processPublications(
      publications: Seq[PostPublishData]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- deduplicator.processPublications(publications)
      trackedReferences = publications.map(publication =>
        publication.publishSource match {
          case PublishSource.Local(messageUuid) =>
            InFlightByMessageId(
              domainId = publication.submissionDomainId,
              messageId = MessageId.fromUuid(messageUuid),
            )

          case PublishSource.Sequencer(requestSequencerCounter, sequencerTimestamp) =>
            InFlightBySequencingInfo(
              domainId = publication.submissionDomainId,
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

  /** Deletes the published, sequenced in-flight submissions with sequencing timestamps up to the given bound
    * and informs the [[CommandDeduplicator]] about the published events.
    *
    * @param upToInclusive Upper bound on the sequencing time of the submissions to be recovered.
    *                      The [[com.digitalasset.canton.ledger.participant.state.Update]]s for all sequenced submissions
    *                      up to this bound must have been published to the indexer.
    *                      The [[com.digitalasset.canton.ledger.participant.state.Update]]s for all sequenced submissions
    *                      in the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] must not yet
    *                      have been pruned from the index store.
    */
  def recoverDomain(domainId: DomainId, upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val domainState = domainStates(domainId).getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(s"Domain state for $domainId not found during crash recovery.")
      )
    )
    for {
      // Re-request ticks for all remaining unsequenced timestamps
      unsequencedInFlights <- store.value.lookupUnsequencedUptoUnordered(
        domainId,
        CantonTimestamp.MaxValue,
      )
      _ = if (unsequencedInFlights.nonEmpty) {
        domainState.domainTimeTracker.requestTicks(
          unsequencedInFlights.map(_.sequencingInfo.timeout)
        )
      }
    } yield ()
  }

  private def domainStateFor(
      domainId: DomainId
  ): EitherT[Future, UnknownDomain, InFlightSubmissionTrackerDomainState] =
    EitherT(Future.successful {
      domainStates(domainId).toRight(UnknownDomain(domainId))
    })
}

object InFlightSubmissionTracker {

  /** The portion of the [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState]]
    * that is used by the [[InFlightSubmissionTracker]].
    *
    * @param observedTimestampTracker
    *   Tracks the observed timestamps per domain to synchronize submission registration with submission deletion.
    *   We track them per domain so that clock skew between different domains does not cause interferences.
    * @param domainTimeTracker
    *   Used to request a timestamp observation for the
    *   [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]s of
    *   [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]]s.
    */
  final case class InFlightSubmissionTrackerDomainState(
      observedTimestampTracker: WatermarkTracker[CantonTimestamp],
      domainTimeTracker: DomainTimeTracker,
  )

  object InFlightSubmissionTrackerDomainState {

    def fromSyncDomainState(
        ephemeral: SyncDomainEphemeralState
    ): InFlightSubmissionTrackerDomainState =
      InFlightSubmissionTrackerDomainState(
        ephemeral.observedTimestampTracker,
        ephemeral.timeTracker,
      )
  }

  sealed trait InFlightSubmissionTrackerError extends Product with Serializable

  final case class SubmissionAlreadyInFlight(
      newSubmission: InFlightSubmission[UnsequencedSubmission],
      existingSubmission: InFlightSubmission[SubmissionSequencingInfo],
  ) extends InFlightSubmissionTrackerError

  final case class TimeoutTooLow(
      submission: InFlightSubmission[UnsequencedSubmission],
      lowerBound: CantonTimestamp,
  ) extends InFlightSubmissionTrackerError

  final case class UnknownDomain(domainId: DomainId) extends InFlightSubmissionTrackerError
}

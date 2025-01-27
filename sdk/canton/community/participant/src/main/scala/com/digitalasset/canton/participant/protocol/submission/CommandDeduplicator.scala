// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod, Offset}
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.{
  AlreadyExists,
  DeduplicationFailed,
  DeduplicationPeriodTooEarly,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.ExecutionContext

/** Implements the command deduplication logic.
  *
  * All method calls should be coordinated by the [[InFlightSubmissionTracker]].
  * In particular, `checkDeduplication` must not be called concurrently with
  * `processPublications` for the same [[com.digitalasset.canton.ledger.participant.state.ChangeId]]s.
  */
trait CommandDeduplicator {

  /** Register the publication of the events in the [[com.digitalasset.canton.participant.store.CommandDeduplicationStore]] */
  def processPublications(
      publications: Seq[PostPublishData]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Perform deduplication for the given [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]
    * and [[com.digitalasset.canton.data.DeduplicationPeriod]].
    *
    * @param changeIdHash The change ID hash of the submission to be deduplicated
    * @param deduplicationPeriod The deduplication period specified with the submission
    * @return The [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset]]
    *         to be included in the command completion's [[com.digitalasset.canton.ledger.participant.state.CompletionInfo]].
    *         Canton always returns a [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset]]
    *         because it cannot meet the record time requirements for the other kinds of
    *         [[com.digitalasset.canton.data.DeduplicationPeriod]]s.
    */
  def checkDuplication(changeIdHash: ChangeIdHash, deduplicationPeriod: DeduplicationPeriod)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
}

object CommandDeduplicator {
  sealed trait DeduplicationFailed extends Product with Serializable

  final case class AlreadyExists(
      completionOffset: Offset,
      accepted: Boolean,
      existingSubmissionId: Option[LedgerSubmissionId],
  ) extends DeduplicationFailed

  final case class DeduplicationPeriodTooEarly(
      requested: DeduplicationPeriod,
      earliestDeduplicationStart: DeduplicationPeriod,
  ) extends DeduplicationFailed
}

class CommandDeduplicatorImpl(
    store: Eval[CommandDeduplicationStore],
    clock: Clock,
    publicationTimeLowerBound: Eval[CantonTimestamp],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends CommandDeduplicator
    with NamedLogging {

  override def processPublications(
      publications: Seq[PostPublishData]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.value.storeDefiniteAnswers(
      publications.map(publication =>
        (
          ChangeId(
            applicationId = publication.applicationId,
            commandId = publication.commandId,
            actAs = publication.actAs,
          ),
          DefiniteAnswerEvent(
            offset = publication.offset,
            publicationTime = publication.publicationTime,
            submissionIdO = publication.submissionId,
          )(publication.traceContext),
          publication.accepted,
        )
      )
    )

  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] = {

    def dedupPeriodTooEarly(prunedOffset: Offset): DeduplicationFailed =
      DeduplicationPeriodTooEarly(
        deduplicationPeriod,
        DeduplicationPeriod.DeduplicationOffset(
          // We may hand out the latest pruned offset because deduplication offsets are exclusive
          Some(prunedOffset)
        ),
      )

    def dedupDuration(
        duration: java.time.Duration
    ): EitherT[FutureUnlessShutdown, DeduplicationFailed, Option[Offset]] = {
      // Convert the duration into a timestamp based on the local participant clock
      // and check against the publication time of the definite answer events.
      //
      // Set the baseline to at least the to-be-assigned publication time
      // so that durations up to the max deduplication duration always fall
      // into the unpruned window.
      //
      // By including `clock.now`, it may happen that the assigned publication time is actually lower than
      // the baseline, e.g., if the request is sequenced and then processing fails over to a participant
      // where the clock lags behind.  We accept this for now as the deduplication guarantee
      // does not forbid clocks that run backwards. Including `clock.now` ensures that time advances
      // for deduplication even if no events happen on the synchronizer.
      val baseline =
        Ordering[CantonTimestamp].max(clock.now, publicationTimeLowerBound.value).toInstant

      def checkAgainstPruning(
          deduplicationStart: CantonTimestamp
      ): EitherT[FutureUnlessShutdown, DeduplicationFailed, Option[Offset]] =
        EitherTUtil.leftSubflatMap(store.value.latestPruning().toLeft(Option.empty[Offset])) {
          case OffsetAndPublicationTime(prunedOffset, prunedPublicationTime) =>
            Either.cond(
              deduplicationStart > prunedPublicationTime,
              Some(prunedOffset),
              dedupPeriodTooEarly(prunedOffset),
            )
        }

      for {
        deduplicationStart <- EitherT.fromEither[FutureUnlessShutdown](
          CantonTimestamp.fromInstant(baseline.minus(duration)).leftMap[DeduplicationFailed] {
            err =>
              logger.info(s"Deduplication period underflow: $err")
              val longestDeduplicationPeriod =
                java.time.Duration.between(CantonTimestamp.MinValue.toInstant, baseline)
              DeduplicationPeriodTooEarly(
                deduplicationPeriod,
                DeduplicationPeriod.DeduplicationDuration(longestDeduplicationPeriod),
              )
          }
        )
        dedupEntryO <- EitherT.right(store.value.lookup(changeIdHash).value)
        reportedDedupOffset <- dedupEntryO match {
          case None =>
            checkAgainstPruning(deduplicationStart)
          case Some(CommandDeduplicationData(_changeId, _latestDefiniteAnswer, latestAcceptance)) =>
            // TODO(#7348) add submission rank check using latestDefiniteAnswer
            latestAcceptance match {
              case None => checkAgainstPruning(deduplicationStart)
              case Some(acceptance) =>
                EitherT.cond[FutureUnlessShutdown](
                  acceptance.publicationTime < deduplicationStart,
                  // Use the found offset rather than deduplicationStart
                  // so that we don't have to check whether deduplicationStart is at most the ledger end
                  Some(acceptance.offset),
                  AlreadyExists(
                    acceptance.offset,
                    accepted = true,
                    acceptance.submissionIdO,
                  ): DeduplicationFailed,
                )
            }
        }
      } yield reportedDedupOffset
    }

    def dedupOffset(
        dedupOffset: Option[Offset]
    ): EitherT[FutureUnlessShutdown, DeduplicationFailed, Option[Offset]] = {
      def checkAgainstPruning()
          : EitherT[FutureUnlessShutdown, DeduplicationFailed, Option[Offset]] =
        EitherTUtil.leftSubflatMap(store.value.latestPruning().toLeft(Option.empty[Offset])) {
          case OffsetAndPublicationTime(prunedOffset, _prunedPublicationTime) =>
            Either.cond(
              Option(prunedOffset) <= dedupOffset,
              Some(prunedOffset),
              dedupPeriodTooEarly(prunedOffset),
            )
        }

      for {
        dedupEntryO <- EitherT.right(store.value.lookup(changeIdHash).value)
        reportedDedupOffset <- dedupEntryO match {
          case None => checkAgainstPruning()
          case Some(CommandDeduplicationData(_changeId, _latestDefiniteAnswer, latestAcceptance)) =>
            // TODO(#7348) add submission rank check using latestDefiniteAnswer
            latestAcceptance match {
              case None => checkAgainstPruning()
              case Some(acceptance) =>
                EitherT.cond[FutureUnlessShutdown](
                  Option(acceptance.offset) <= dedupOffset,
                  Some(acceptance.offset),
                  AlreadyExists(
                    acceptance.offset,
                    accepted = true,
                    acceptance.submissionIdO,
                  ): DeduplicationFailed,
                )
            }
        }
      } yield reportedDedupOffset
    }

    val dedupOffsetE = deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) => dedupDuration(duration)
      case DeduplicationPeriod.DeduplicationOffset(offset) => dedupOffset(offset)
    }
    dedupOffsetE.map(dedupOffset => DeduplicationPeriod.DeduplicationOffset(dedupOffset))
  }
}

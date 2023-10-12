// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.{
  AlreadyExists,
  DeduplicationFailed,
  DeduplicationPeriodTooEarly,
  MalformedOffset,
}
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncOffset}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

/** Implements the command deduplication logic.
  *
  * All method calls should be coordinated by the [[InFlightSubmissionTracker]].
  * In particular, `checkDeduplication` must not be called concurrently with
  * `processPublications` for the same [[com.digitalasset.canton.ledger.participant.state.v2.ChangeId]]s.
  */
trait CommandDeduplicator {

  /** Register the publication of the events in the [[com.digitalasset.canton.participant.store.CommandDeduplicationStore]] */
  def processPublications(
      publications: Seq[MultiDomainEventLog.OnPublish.Publication]
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Perform deduplication for the given [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]
    * and [[com.digitalasset.canton.ledger.api.DeduplicationPeriod]].
    *
    * @param changeIdHash The change ID hash of the submission to be deduplicated
    * @param deduplicationPeriod The deduplication period specified with the submission
    *
    * @return The [[com.digitalasset.canton.ledger.api.DeduplicationPeriod.DeduplicationOffset]]
    *         to be included in the command completion's [[com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo]].
    *         Canton always returns a [[com.digitalasset.canton.ledger.api.DeduplicationPeriod.DeduplicationOffset]]
    *         because it cannot meet the record time requirements for the other kinds of
    *         [[com.digitalasset.canton.ledger.api.DeduplicationPeriod]]s.
    */
  def checkDuplication(changeIdHash: ChangeIdHash, deduplicationPeriod: DeduplicationPeriod)(
      implicit traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
}

object CommandDeduplicator {
  sealed trait DeduplicationFailed extends Product with Serializable

  final case class MalformedOffset(error: String) extends DeduplicationFailed

  final case class AlreadyExists(
      completionOffset: GlobalOffset,
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
      publications: Seq[MultiDomainEventLog.OnPublish.Publication]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val offsetsAndCompletionInfos = publications.mapFilter {
      case MultiDomainEventLog.OnPublish.Publication(
            globalOffset,
            publicationTime,
            _inFlightReferenceO,
            deduplicationInfoO,
            _event,
          ) =>
        deduplicationInfoO.map { dedupInfo =>
          (
            dedupInfo.changeId,
            DefiniteAnswerEvent(
              globalOffset,
              publicationTime,
              dedupInfo.submissionId,
            )(dedupInfo.eventTraceContext),
            dedupInfo.acceptance,
          )
        }
    }
    store.value.storeDefiniteAnswers(offsetsAndCompletionInfos)
  }

  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] = {

    def dedupPeriodTooEarly(prunedOffset: GlobalOffset): DeduplicationFailed =
      DeduplicationPeriodTooEarly(
        deduplicationPeriod,
        DeduplicationPeriod.DeduplicationOffset(
          // We may hand out the latest pruned offset because deduplication offsets are exclusive
          UpstreamOffsetConvert.fromGlobalOffset(prunedOffset)
        ),
      )

    // If we haven't pruned the command deduplication store, this store contains all command deduplication entries
    // and can handle any reasonable timestamp and offset.
    // Ideally, we would report the predecessor to the first ledger offset
    // as deduplication offsets are exclusive, but this would be an invalid Canton global offset.
    // So we report the first ledger offset as the deduplication start instead.
    // This difference does not matter in practice for command deduplication
    // as the first offset always contains the ledger configuration and can therefore never be a command completion.
    def unprunedDedupOffset: GlobalOffset = MultiDomainEventLog.ledgerFirstOffset

    def dedupDuration(
        duration: java.time.Duration
    ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
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
      // for deduplication even if no events happen on the domain.
      val baseline =
        Ordering[CantonTimestamp].max(clock.now, publicationTimeLowerBound.value).toInstant

      def checkAgainstPruning(
          deduplicationStart: CantonTimestamp
      ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
        EitherTUtil.leftSubflatMap(store.value.latestPruning().toLeft(unprunedDedupOffset)) {
          case OffsetAndPublicationTime(prunedOffset, prunedPublicationTime) =>
            Either.cond(
              deduplicationStart > prunedPublicationTime,
              prunedOffset,
              dedupPeriodTooEarly(prunedOffset),
            )
        }
      }

      for {
        deduplicationStart <- EitherT.fromEither[Future](
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
                EitherT.cond[Future](
                  acceptance.publicationTime < deduplicationStart,
                  // Use the found offset rather than deduplicationStart
                  // so that we don't have to check whether deduplicationStart is at most the ledger end
                  acceptance.offset,
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
        offset: LedgerSyncOffset
    ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
      def checkAgainstPruning(
          dedupOffset: GlobalOffset
      ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
        EitherTUtil.leftSubflatMap(store.value.latestPruning().toLeft(unprunedDedupOffset)) {
          case OffsetAndPublicationTime(prunedOffset, prunedPublicationTime) =>
            Either.cond(
              prunedOffset <= dedupOffset,
              prunedOffset,
              dedupPeriodTooEarly(prunedOffset),
            )
        }
      }

      for {
        dedupOffset <- EitherT.fromEither[Future](
          UpstreamOffsetConvert
            .toGlobalOffset(offset)
            .leftMap[DeduplicationFailed](err => MalformedOffset(err))
        )
        dedupEntryO <- EitherT.right(store.value.lookup(changeIdHash).value)
        reportedDedupOffset <- dedupEntryO match {
          case None =>
            checkAgainstPruning(dedupOffset)
          case Some(CommandDeduplicationData(_changeId, _latestDefiniteAnswer, latestAcceptance)) =>
            // TODO(#7348) add submission rank check using latestDefiniteAnswer
            latestAcceptance match {
              case None => checkAgainstPruning(dedupOffset)
              case Some(acceptance) =>
                EitherT.cond[Future](
                  acceptance.offset <= dedupOffset,
                  acceptance.offset,
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
    dedupOffsetE.map(dedupOffset =>
      DeduplicationPeriod.DeduplicationOffset(UpstreamOffsetConvert.fromGlobalOffset(dedupOffset))
    )
  }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MapsUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** In-memory implementation of [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] */
class InMemoryInFlightSubmissionStore(override protected val loggerFactory: NamedLoggerFactory)(
    implicit executionContext: ExecutionContext
) extends InFlightSubmissionStore
    with NamedLogging {

  /** Invariant: The [[com.digitalasset.canton.participant.protocol.submission.ChangeId]] of
    * a value is the key.
    */
  private val inFlights
      : concurrent.Map[ChangeIdHash, InFlightSubmission[SubmissionSequencingInfo]] =
    new TrieMap[ChangeIdHash, InFlightSubmission[SubmissionSequencingInfo]]

  override def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo]] =
    OptionT(FutureUnlessShutdown.pure(inFlights.get(changeIdHash)))

  override def lookupEarliest(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure {
      inFlights.valuesIterator.foldLeft(Option.empty[CantonTimestamp]) { (previousO, entry) =>
        if (entry.submissionSynchronizerId == synchronizerId) {
          val next = previousO.fold(entry.associatedTimestamp) { previous =>
            Ordering[CantonTimestamp].min(previous, entry.associatedTimestamp)
          }
          next.some
        } else previousO
      }
    }

  override def register(
      submission: InFlightSubmission[UnsequencedSubmission]
  ): EitherT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo], Unit] =
    inFlights
      .putIfAbsent(submission.changeIdHash, submission)
      .fold(Either.right[InFlightSubmission[SubmissionSequencingInfo], Unit](())) { old =>
        Either.cond(old == submission, (), old)
      }
      .toEitherT[FutureUnlessShutdown]

  override def updateRegistration(
      submission: InFlightSubmission[UnsequencedSubmission],
      rootHash: RootHash,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    inFlights.mapValuesInPlace { (_, info) =>
      if (
        !info.isSequenced && info.submissionSynchronizerId == submission.submissionSynchronizerId
        && info.changeIdHash == submission.changeIdHash && info.rootHashO.isEmpty
      ) {
        info.copy(rootHashO = Some(rootHash))
      } else info
    }
    FutureUnlessShutdown.unit
  }

  override def observeSequencing(
      synchronizerId: SynchronizerId,
      submissions: Map[MessageId, SequencedSubmission],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    inFlights.mapValuesInPlace { (_, info) =>
      if (!info.isSequenced && info.submissionSynchronizerId == synchronizerId) {
        submissions.get(info.messageId).fold(info) { sequencedInfo =>
          info.copy(sequencingInfo = sequencedInfo)
        }
      } else info
    }
    FutureUnlessShutdown.unit
  }

  override def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    inFlights.mapValuesInPlace { (_, info) =>
      val shouldUpdate = info.rootHashO.contains(rootHash) && (info.sequencingInfo match {
        case UnsequencedSubmission(_, _) => true
        case SequencedSubmission(_, ts) => submission.sequencingTime < ts
      })
      if (shouldUpdate) {
        info.copy(sequencingInfo = submission)
      } else info
    }
    FutureUnlessShutdown.unit
  }

  override def delete(
      submissions: Seq[InFlightReference]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      val (byId, bySequencingInfo) = submissions.toList.map(_.toEither).separate
      inFlights.filterInPlace { case (_, inFlightSubmission) =>
        !(inFlightSubmission.sequencingInfo.asSequenced.exists { sequenced =>
          bySequencingInfo.contains(
            InFlightBySequencingInfo(inFlightSubmission.submissionSynchronizerId, sequenced)
          )
        } || byId.contains(inFlightSubmission.referenceByMessageId))
      }
    }

  override def updateUnsequenced(
      changeIdHash: ChangeIdHash,
      submissionDomain: SynchronizerId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    MapsUtil
      .updateWithConcurrently(inFlights, changeIdHash) { submission =>
        submission.sequencingInfo.asUnsequenced match {
          case Some(unsequenced) =>
            if (
              submission.submissionSynchronizerId != submissionDomain || submission.messageId != messageId
            )
              submission
            else if (unsequenced.timeout < newSequencingInfo.timeout) {
              logger.warn(
                show"Sequencing timeout for submission (change ID hash $changeIdHash, message Id $messageId on $submissionDomain) is at ${unsequenced.timeout} before ${newSequencingInfo.timeout}. Current data: $unsequenced"
              )
              submission
            } else {
              submission.copy(sequencingInfo = newSequencingInfo)
            }
          case None =>
            logger.warn(
              show"Submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain has already been sequenced. ${submission.sequencingInfo}"
            )
            submission
        }
      }
      .discard
    FutureUnlessShutdown.unit
  }

  override def lookupUnsequencedUptoUnordered(
      synchronizerId: SynchronizerId,
      observedSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[InFlightSubmission[UnsequencedSubmission]]] =
    FutureUnlessShutdown.pure {
      val unsequenced = Seq.newBuilder[InFlightSubmission[UnsequencedSubmission]]
      inFlights.values.foreach { submission =>
        if (submission.submissionSynchronizerId == synchronizerId) {
          submission
            .traverseSequencingInfo(_.asUnsequenced.filter(_.timeout <= observedSequencingTime))
            .foreach { unsequencedSubmission =>
              unsequenced += unsequencedSubmission
            }
        }
      }
      unsequenced.result()
    }

  override def lookupSequencedUptoUnordered(
      synchronizerId: SynchronizerId,
      sequencingTimeInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[InFlightSubmission[SequencedSubmission]]] =
    FutureUnlessShutdown.pure {
      val sequenced = Seq.newBuilder[InFlightSubmission[SequencedSubmission]]
      inFlights.values.foreach { submission =>
        if (submission.submissionSynchronizerId == synchronizerId) {
          submission
            .traverseSequencingInfo(
              _.asSequenced.filter(_.sequencingTime <= sequencingTimeInclusive)
            )
            .foreach { sequencedSubmission =>
              sequenced += sequencedSubmission
            }
        }
      }
      sequenced.result()
    }

  override def lookupSomeMessageId(synchronizerId: SynchronizerId, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[InFlightSubmission[SubmissionSequencingInfo]]] =
    FutureUnlessShutdown.pure {
      inFlights.collectFirst {
        case (_changeIdHash, inFlight)
            if inFlight.submissionSynchronizerId == synchronizerId && inFlight.messageId == messageId =>
          inFlight
      }
    }

  override def close(): Unit = ()
}

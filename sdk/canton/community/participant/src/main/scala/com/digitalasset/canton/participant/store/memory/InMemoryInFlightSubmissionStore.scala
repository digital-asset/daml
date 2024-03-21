// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.data.CantonTimestamp
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MapsUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** In-memory implementation of [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] */
class InMemoryInFlightSubmissionStore(override protected val loggerFactory: NamedLoggerFactory)(
    implicit executionContext: ExecutionContext
) extends InFlightSubmissionStore
    with NamedLogging {

  /** Invariant: The [[com.digitalasset.canton.participant.protocol.submission.ChangeId]] of
    * a value is the key.
    */
  private val inFlight: concurrent.Map[ChangeIdHash, InFlightSubmission[SubmissionSequencingInfo]] =
    new TrieMap[ChangeIdHash, InFlightSubmission[SubmissionSequencingInfo]]

  override def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, InFlightSubmission[SubmissionSequencingInfo]] =
    OptionT(Future.successful(inFlight.get(changeIdHash)))

  override def lookupEarliest(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = Future.successful {
    inFlight.valuesIterator.foldLeft(Option.empty[CantonTimestamp]) { (previousO, entry) =>
      if (entry.submissionDomain == domainId) {
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
    inFlight
      .putIfAbsent(submission.changeIdHash, submission)
      .fold(Either.right[InFlightSubmission[SubmissionSequencingInfo], Unit](())) { old =>
        Either.cond(old == submission, (), old)
      }
      .toEitherT[FutureUnlessShutdown]

  override def updateRegistration(
      submission: InFlightSubmission[UnsequencedSubmission],
      rootHash: RootHash,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    inFlight.mapValuesInPlace { (_changeId, info) =>
      if (
        !info.isSequenced && info.submissionDomain == submission.submissionDomain
        && info.changeIdHash == submission.changeIdHash && info.rootHashO.isEmpty
      ) {
        info.copy(rootHashO = Some(rootHash))
      } else info
    }
    Future.unit
  }

  override def observeSequencing(
      domainId: DomainId,
      submissions: Map[MessageId, SequencedSubmission],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    inFlight.mapValuesInPlace { (_changeId, info) =>
      if (!info.isSequenced && info.submissionDomain == domainId) {
        submissions.get(info.messageId).fold(info) { sequencedInfo =>
          info.copy(sequencingInfo = sequencedInfo)
        }
      } else info
    }
    Future.unit
  }

  override def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    inFlight.mapValuesInPlace { (_changeId, info) =>
      val shouldUpdate = info.rootHashO.contains(rootHash) && (info.sequencingInfo match {
        case UnsequencedSubmission(_, _) => true
        case SequencedSubmission(_sc, ts) => submission.sequencingTime < ts
      })
      if (shouldUpdate) {
        info.copy(sequencingInfo = submission)
      } else info
    }
    Future.unit
  }

  override def delete(
      submissions: Seq[InFlightReference]
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      val (byId, bySequencingInfo) = submissions.toList.map(_.toEither).separate
      inFlight.filterInPlace { case (_changeIdHash, inFlightSubmission) =>
        !(inFlightSubmission.sequencingInfo.asSequenced.exists { sequenced =>
          bySequencingInfo.contains(
            InFlightBySequencingInfo(inFlightSubmission.submissionDomain, sequenced)
          )
        } || byId.contains(inFlightSubmission.referenceByMessageId))
      }
    }

  override def updateUnsequenced(
      changeIdHash: ChangeIdHash,
      submissionDomain: DomainId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    MapsUtil
      .updateWithConcurrently(inFlight, changeIdHash) { submission =>
        submission.sequencingInfo.asUnsequenced match {
          case Some(unsequenced) =>
            if (
              submission.submissionDomain != submissionDomain || submission.messageId != messageId
            )
              submission
            else if (unsequenced.timeout < newSequencingInfo.timeout) {
              logger.warn(
                show"Sequencing timeout for submission (change ID hash $changeIdHash, message Id $messageId on $submissionDomain) is at ${unsequenced.timeout} before ${newSequencingInfo.timeout}. Current data: ${unsequenced}"
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
    Future.unit
  }

  override def lookupUnsequencedUptoUnordered(
      domainId: DomainId,
      observedSequencingTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Seq[InFlightSubmission[UnsequencedSubmission]]] =
    Future.successful {
      val unsequenced = Seq.newBuilder[InFlightSubmission[UnsequencedSubmission]]
      inFlight.values.foreach { submission =>
        if (submission.submissionDomain == domainId) {
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
      domainId: DomainId,
      sequencingTimeInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Seq[InFlightSubmission[SequencedSubmission]]] =
    Future.successful {
      val sequenced = Seq.newBuilder[InFlightSubmission[SequencedSubmission]]
      inFlight.values.foreach { submission =>
        if (submission.submissionDomain == domainId) {
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

  override def lookupSomeMessageId(domainId: DomainId, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Future[Option[InFlightSubmission[SubmissionSequencingInfo]]] =
    Future.successful {
      inFlight.collectFirst {
        case (changeIdHash, inFlight)
            if inFlight.submissionDomain == domainId && inFlight.messageId == messageId =>
          inFlight
      }
    }

  override def close(): Unit = ()
}

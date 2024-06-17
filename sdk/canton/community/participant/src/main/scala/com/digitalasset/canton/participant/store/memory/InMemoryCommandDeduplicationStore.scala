// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.OptionT
import cats.syntax.option.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.ChangeIdHash
import com.digitalasset.canton.participant.store.{
  CommandDeduplicationData,
  CommandDeduplicationStore,
  DefiniteAnswerEvent,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.Try

class InMemoryCommandDeduplicationStore(override protected val loggerFactory: NamedLoggerFactory)
    extends CommandDeduplicationStore
    with NamedLogging {

  import CommandDeduplicationStore.*

  /** The command deduplication data for each change ID unless pruned. */
  private val byChangeId: TrieMap[ChangeIdHash, CommandDeduplicationData] =
    new TrieMap[ChangeIdHash, CommandDeduplicationData]()

  /** The highest pruning offset and an upper bound on the publishing time of pruned offsets. */
  private val latestPrunedRef: AtomicReference[Option[OffsetAndPublicationTime]] =
    new AtomicReference[Option[OffsetAndPublicationTime]](None)

  override def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, CommandDeduplicationData] =
    OptionT(Future.successful(byChangeId.get(changeIdHash)))

  override def storeDefiniteAnswers(
      answers: Seq[(ChangeId, DefiniteAnswerEvent, Boolean)]
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.fromTry(Try {
      answers.foreach { case (changeId, definiteAnswerEvent, accepted) =>
        storeDefiniteAnswerInternal(changeId, definiteAnswerEvent, accepted)
      }
    })

  private def storeDefiniteAnswerInternal(
      changeId: ChangeId,
      definiteAnswerEvent: DefiniteAnswerEvent,
      accepted: Boolean,
  ): Unit = {
    implicit val traceContext: TraceContext = definiteAnswerEvent.traceContext
    val changeIdHash = ChangeIdHash(changeId)
    val newEntry = byChangeId.updateWith(changeIdHash) {
      case None =>
        val dedupData = checked(
          CommandDeduplicationData
            .tryCreate(
              changeId,
              definiteAnswerEvent,
              if (accepted) definiteAnswerEvent.some else None,
            )
        )
        dedupData.some
      case Some(oldData) =>
        if (oldData.latestDefiniteAnswer.offset > definiteAnswerEvent.offset) {
          // The InFlightSubmissionTracker should make sure that there is always at most one submission in-flight for each change ID.
          // Since the MultiDomainEventLog assigns global offsets in ascending order,
          // we should not end up with a completion from a later request overtaking the completion of an earlier request
          // for the same change ID.
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Cannot update command deduplication data for $changeIdHash from offset ${oldData.latestDefiniteAnswer.offset} to offset ${definiteAnswerEvent.offset}"
            )
          )
        } else {
          val newLatestAcceptance =
            if (accepted) definiteAnswerEvent.some else oldData.latestAcceptance
          checked(
            CommandDeduplicationData.tryCreate(changeId, definiteAnswerEvent, newLatestAcceptance)
          ).some
        }
    }
    logger.debug(
      s"Command deduplication data for $changeIdHash is now ${newEntry.fold("none")(_.toString)}"
    )
  }

  override def prune(upToInclusive: GlobalOffset, prunedPublicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    latestPrunedRef.getAndUpdate {
      case None => OffsetAndPublicationTime(upToInclusive, prunedPublicationTime).some
      case oldData @ Some(OffsetAndPublicationTime(oldOffset, oldPrunedPublicationTime)) =>
        if (oldOffset < upToInclusive) {
          OffsetAndPublicationTime(
            upToInclusive,
            Ordering[CantonTimestamp].max(prunedPublicationTime, oldPrunedPublicationTime),
          ).some
        } else if (oldPrunedPublicationTime < prunedPublicationTime) {
          OffsetAndPublicationTime(oldOffset, prunedPublicationTime).some
        } else oldData
    }
    byChangeId.filterInPlace { case (_changeIdHash, dedupData) =>
      // since the acceptance offset is bounded by the latestDefiniteAnswer offset, we do not need to check acceptance offset
      dedupData.latestDefiniteAnswer.offset > upToInclusive
    }
    Future.unit
  }

  override def latestPruning()(implicit
      traceContext: TraceContext
  ): OptionT[Future, OffsetAndPublicationTime] =
    OptionT(Future.successful(latestPrunedRef.get()))

  override def close(): Unit = ()
}

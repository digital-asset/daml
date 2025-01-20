// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.syntax.option.*
import com.daml.lf.data.Ref
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.{
  AlreadyExists,
  DeduplicationPeriodTooEarly,
  MalformedOffset,
}
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.DeduplicationInfo
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.InMemoryCommandDeduplicationStore
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.CommandRejected.FinalReason
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.{
  CommandRejected,
  TransactionAccepted,
}
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.participant.{
  DefaultParticipantStateValues,
  GlobalOffset,
  LedgerSyncOffset,
  LocalOffset,
}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, RequestCounter}
import com.google.rpc.Code
import com.google.rpc.status.Status as RpcStatus
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
class CommandDeduplicatorTest extends AsyncWordSpec with BaseTest {
  import scala.language.implicitConversions

  private lazy val clock = new SimClock(loggerFactory = loggerFactory)

  private lazy val submissionId1 = DefaultDamlValues.submissionId().some
  private lazy val event1 = TransactionAccepted(
    completionInfoO = DefaultParticipantStateValues.completionInfo(List.empty).some,
    transactionMeta = DefaultParticipantStateValues.transactionMeta(),
    transaction = DefaultDamlValues.emptyCommittedTransaction,
    transactionId = DefaultDamlValues.lfTransactionId(1),
    recordTime = CantonTimestamp.Epoch.toLf,
    divulgedContracts = List.empty,
    blindingInfoO = None,
    hostedWitnesses = Nil,
    contractMetadata = Map(),
  )
  private lazy val changeId1 = event1.completionInfoO.value.changeId
  private lazy val changeId1Hash = ChangeIdHash(changeId1)

  private lazy val event2 =
    event1.copy(completionInfoO = None, transactionId = DefaultDamlValues.lfTransactionId(2))

  private lazy val event1reject = CommandRejected(
    CantonTimestamp.Epoch.toLf,
    event1.completionInfoO.value,
    new FinalReason(RpcStatus(code = Code.ABORTED_VALUE, message = "event1 rejection")),
    ProcessingSteps.RequestType.Transaction,
    Some(domainId),
  )

  private lazy val event3 = CommandRejected(
    CantonTimestamp.Epoch.toLf,
    DefaultParticipantStateValues
      .completionInfo(List.empty, commandId = DefaultDamlValues.commandId(3), submissionId = None),
    FinalReason(RpcStatus(code = Code.NOT_FOUND_VALUE, message = "event3 message")),
    ProcessingSteps.RequestType.Transaction,
    Some(domainId),
  )
  private lazy val changeId3 = event3.completionInfo.changeId
  private lazy val changeId3Hash = ChangeIdHash(changeId3)

  private lazy val domainId = DefaultTestIdentities.domainId
  private lazy val messageId = MessageId.fromUuid(new UUID(10, 10))
  private lazy val inFlightReference = InFlightByMessageId(domainId, messageId)

  class Fixture(
      val dedup: CommandDeduplicator,
      val store: CommandDeduplicationStore,
  )

  private lazy val eventsInLog = Seq(event1reject, event1, event1reject, event2, event3)
  private lazy val Seq(
    event1rejectOffset,
    event1Offset,
    event1rejectOffset2,
    event2Offset,
    event3Offset,
  ) = eventsInLog.zipWithIndex.map(_._2.toLong)

  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  private implicit def toLocalOffset(i: Long): LocalOffset =
    LocalOffset(RequestCounter(i))

  private def mk(lowerBound: CantonTimestamp = CantonTimestamp.MinValue): Fixture = {
    val store = new InMemoryCommandDeduplicationStore(loggerFactory)
    val dedup =
      new CommandDeduplicatorImpl(Eval.now(store), clock, Eval.now(lowerBound), loggerFactory)
    new Fixture(dedup, store)
  }

  private def dedupOffset(globalOffset: GlobalOffset): DeduplicationPeriod.DeduplicationOffset =
    DeduplicationPeriod.DeduplicationOffset(UpstreamOffsetConvert.fromGlobalOffset(globalOffset))

  private def mkPublication(
      globalOffset: GlobalOffset,
      localOffset: LocalOffset,
      publicationTime: CantonTimestamp,
      inFlightReference: Option[InFlightReference] = this.inFlightReference.some,
  ): MultiDomainEventLog.OnPublish.Publication = {
    val deduplicationInfo =
      if (localOffset.toLong < eventsInLog.size) {
        DeduplicationInfo.fromEvent(
          eventsInLog(localOffset.toLong.toInt),
          TraceContext.empty,
        )
      } else None

    MultiDomainEventLog.OnPublish.Publication(
      globalOffset,
      publicationTime,
      inFlightReference,
      deduplicationInfo,
      event1,
    )
  }

  private lazy val dedupTime1Day: DeduplicationPeriod.DeduplicationDuration =
    DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ofDays(1))
  private lazy val dedupTimeAlmost1Day: DeduplicationPeriod.DeduplicationDuration =
    DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ofDays(1).minusNanos(1000))

  "CommandDeduplicator" should {
    "accept fresh change IDs" in {
      val fix = mk()
      for {
        offset1 <- fix.dedup
          .checkDuplication(
            changeId1Hash,
            DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ofDays(1)),
          )
          .valueOrFail("dedup 1")
        offset3 <- fix.dedup
          .checkDuplication(changeId3Hash, dedupOffset(100L))
          .valueOrFail("dedup 3")
      } yield {
        offset1 shouldBe dedupOffset(MultiDomainEventLog.ledgerFirstOffset)
        offset3 shouldBe dedupOffset(MultiDomainEventLog.ledgerFirstOffset)
      }
    }

    "complain about malformed offsets" in {
      val fix = mk()
      val malformedOffset =
        DeduplicationPeriod.DeduplicationOffset(
          LedgerSyncOffset.fromHexString(Ref.HexString.assertFromString("0123456789abcdef00"))
        )
      for {
        error <- fix.dedup
          .checkDuplication(changeId1Hash, malformedOffset)
          .leftOrFail("malformed offset")
      } yield error shouldBe a[MalformedOffset]
    }

    "complain about time underflow" in {
      val fix = mk()
      val timeUnderflowDuration =
        DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ofDays(365 * 10000))
      for {
        error <- fix.dedup
          .checkDuplication(changeId1Hash, timeUnderflowDuration)
          .leftOrFail("time underflow")

      } yield {
        error shouldBe DeduplicationPeriodTooEarly(
          timeUnderflowDuration,
          DeduplicationPeriod.DeduplicationDuration(
            java.time.Duration.between(CantonTimestamp.MinValue.toInstant, clock.now.toInstant)
          ),
        )
      }
    }

    "persist publication" in {
      val fix = mk()
      val offset1 = 3L
      val publicationTime1 = CantonTimestamp.ofEpochSecond(1)
      val publication1 = mkPublication(offset1, event1Offset, publicationTime1)
      val offset2 = 4L
      val publicationTime2 = CantonTimestamp.ofEpochSecond(2)
      val publication2 = mkPublication(offset2, event3Offset, publicationTime2)
      val offset3 = 6L
      val publicationTime3 = CantonTimestamp.ofEpochSecond(3)
      val publication3 = mkPublication(offset3, event1rejectOffset, publicationTime3)
      val publication4 = mkPublication(
        100L,
        event2Offset,
        CantonTimestamp.ofEpochSecond(4),
        inFlightReference = None,
      )
      for {
        () <- fix.dedup.processPublications(Seq(publication1, publication2))
        lookup1 <- valueOrFail(fix.store.lookup(changeId1Hash))("find event1")
        lookup2 <- valueOrFail(fix.store.lookup(changeId3Hash))("find event3")
        () <- fix.dedup.processPublications(
          Seq(publication3, publication4)
        ) // update changeId1 and ignore event2
        lookup3 <- valueOrFail(fix.store.lookup(changeId1Hash))("find event1")
      } yield {
        val definiteAnswerEvent1 =
          DefiniteAnswerEvent(offset1, publicationTime1, submissionId1)(TraceContext.empty)
        lookup1 shouldBe CommandDeduplicationData.tryCreate(
          changeId1,
          definiteAnswerEvent1,
          definiteAnswerEvent1.some,
        )
        val definiteAnswerEvent2 =
          DefiniteAnswerEvent(offset2, publicationTime2, None)(TraceContext.empty)
        lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId3, definiteAnswerEvent2, None)
        val definiteAnswerEvent3 =
          DefiniteAnswerEvent(offset3, publicationTime3, submissionId1)(TraceContext.empty)
        lookup3 shouldBe CommandDeduplicationData.tryCreate(
          changeId1,
          definiteAnswerEvent3,
          definiteAnswerEvent1.some,
        )
      }
    }

    "deduplicate accepted commands" in {
      val fix = mk()
      val publicationTime = clock.now
      val offset1 = 3L
      val publication1 = mkPublication(offset1, event1Offset, publicationTime)
      val offset2 = 5L
      val publication2 =
        mkPublication(offset2, event2Offset, publicationTime) // event without completion info

      for {
        () <- fix.dedup.processPublications(Seq(publication1, publication2))
        () = clock.advance(java.time.Duration.ofDays(1))
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup conflict by time")
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("no dedup conflict by time")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(2L))
          .leftOrFail("dedup conflict by offset")
        okOffset4 <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(3L))
          .valueOrFail("no dedup conflict by offset 3")
        okOffset5 <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(4L))
          .valueOrFail("no dedup conflict by offset 4")
        okOther <- fix.dedup
          .checkDuplication(changeId3Hash, dedupOffset(offset1))
          .valueOrFail("do dedup conflict for other change ID")
      } yield {
        errorTime shouldBe AlreadyExists(offset1, accepted = true, submissionId1)
        okTime shouldBe dedupOffset(offset1)
        errorOffset shouldBe AlreadyExists(offset1, accepted = true, submissionId1)
        okOffset4 shouldBe dedupOffset(offset1)
        okOffset5 shouldBe dedupOffset(offset1)
        okOther shouldBe dedupOffset(MultiDomainEventLog.ledgerFirstOffset)
      }
    }

    "not deduplicate upon a rejection" in {
      val fix = mk()
      val publicationTime1 = clock.now
      val offset1 = 3L
      val publication1 = mkPublication(offset1, event1rejectOffset, publicationTime1)

      clock.advance(java.time.Duration.ofHours(1))
      val publicationTime2 = clock.now
      val offset2 = 5L
      val publication2 = mkPublication(offset2, event1Offset, publicationTime2)

      clock.advance(java.time.Duration.ofHours(1))
      val offset3 = 10L
      val publication3 =
        mkPublication(offset3, event1rejectOffset2, publicationTime2, inFlightReference = None)

      for {
        () <- fix.dedup.processPublications(Seq(publication1))
        () = clock.advance(java.time.Duration.ofDays(1))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .valueOrFail("no dedup conflict by time")
        okOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1 - 1))
          .valueOrFail("no dedup conflict by offset")
        () <- fix.dedup.processPublications(Seq(publication2, publication3))
        okTimeAfter <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .valueOrFail("no dedup conflict by time after accept")
        errorTime <- fix.dedup
          .checkDuplication(
            changeId1Hash,
            DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ofHours(25)),
          )
          .leftOrFail("dedup conflict by time")
        okOffsetAfter <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset2))
          .valueOrFail("no dedup conflict by offset at accept")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset2 - 1))
          .leftOrFail("dedup conflict by offset before accept")
      } yield {
        okTime shouldBe dedupOffset(MultiDomainEventLog.ledgerFirstOffset)
        okOffset shouldBe dedupOffset(MultiDomainEventLog.ledgerFirstOffset)
        okTimeAfter shouldBe dedupOffset(offset2)
        errorTime shouldBe AlreadyExists(offset2, accepted = true, submissionId1)
        okOffsetAfter shouldBe dedupOffset(offset2)
        errorOffset shouldBe AlreadyExists(offset2, accepted = true, submissionId1)
      }
    }

    "check pruning bound" in {
      val fix = mk()
      val pruningTime = clock.now
      val pruningOffset = 30L
      clock.advance(java.time.Duration.ofDays(1))
      for {
        () <- fix.store.prune(pruningOffset, pruningTime)
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup time too early")
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("dedup time OK")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(pruningOffset - 1))
          .leftOrFail("dedup offset too early")
        okOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(pruningOffset))
          .valueOrFail("dedup offset OK")
      } yield {
        errorTime shouldBe DeduplicationPeriodTooEarly(
          dedupTime1Day,
          dedupOffset(pruningOffset),
        )
        okTime shouldBe dedupOffset(pruningOffset)
        errorOffset shouldBe DeduplicationPeriodTooEarly(
          dedupOffset(pruningOffset - 1),
          dedupOffset(pruningOffset),
        )
        okOffset shouldBe dedupOffset(pruningOffset)
      }
    }

    "ignore pruning bound when there is an acceptance" in {
      val fix = mk()
      val publicationTime1 = clock.now
      val offset1 = 5L
      val publication1 = mkPublication(offset1, event1Offset, publicationTime1)

      clock.advance(java.time.Duration.ofHours(1))
      val publicationTime2 = clock.now
      val offset2 = 10L
      val publication2 = mkPublication(offset2, event1rejectOffset2, publicationTime2)

      for {
        () <- fix.dedup.processPublications(Seq(publication1))
        () <- fix.dedup.processPublications(Seq(publication2))
        () = clock.advance(java.time.Duration.ofHours(23))
        // This shouldn't prune the entry because the most recent definite answer is after the pruning offset
        // So we can still specify dedup periods that start before the pruning
        pruningOffset = offset1 + 2L
        () <- fix.store.prune(pruningOffset, publicationTime1.plusSeconds(10))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("no dedup conflict on time")
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup conflict on time")
        okOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1))
          .valueOrFail("no dedup conflict on offset")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1 - 1))
          .leftOrFail("dedup conflict on offset")
        otherTime <- fix.dedup
          .checkDuplication(changeId3Hash, dedupTimeAlmost1Day)
          .leftOrFail("dedup time conflict on other")
        otherOffset <- fix.dedup
          .checkDuplication(changeId3Hash, dedupOffset(pruningOffset - 1L))
          .leftOrFail("dedup time conflict on pre-pruning offset")
      } yield {
        okTime shouldBe dedupOffset(offset1)
        errorTime shouldBe AlreadyExists(offset1, accepted = true, submissionId1)
        okOffset shouldBe dedupOffset(offset1)
        errorOffset shouldBe AlreadyExists(offset1, accepted = true, submissionId1)
        otherTime shouldBe DeduplicationPeriodTooEarly(
          dedupTimeAlmost1Day,
          dedupOffset(pruningOffset),
        )
        otherOffset shouldBe DeduplicationPeriodTooEarly(
          dedupOffset(pruningOffset - 1),
          dedupOffset(pruningOffset),
        )
      }
    }

    "consider the publication time bound" in {
      val publicationTime1 = clock.now
      val offset1 = 5L
      val publication1 = mkPublication(offset1, event1Offset, publicationTime1)

      val lowerBound = publicationTime1.add(java.time.Duration.ofDays(1))
      val fix = mk(lowerBound)

      clock.advance(java.time.Duration.ofHours(23))

      for {
        () <- fix.dedup.processPublications(Seq(publication1))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("no dedup conflict on time")
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup conflict on time")
      } yield {
        okTime shouldBe dedupOffset(offset1)
        errorTime shouldBe AlreadyExists(offset1, accepted = true, submissionId1)
      }
    }
  }
}

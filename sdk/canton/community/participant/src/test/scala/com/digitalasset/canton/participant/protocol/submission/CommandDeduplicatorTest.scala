// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.syntax.option.*
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod, Offset}
import com.digitalasset.canton.ledger.participant.state.CompletionInfo
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.{
  AlreadyExists,
  DeduplicationPeriodTooEarly,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.InMemoryCommandDeduplicationStore
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DefaultDamlValues}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

class CommandDeduplicatorTest extends AsyncWordSpec with BaseTest {
  import scala.language.implicitConversions

  private lazy val clock = new SimClock(loggerFactory = loggerFactory)

  private val domainId = DomainId.tryFromString("da::default")
  private lazy val submissionId1 = DefaultDamlValues.submissionId().some
  private lazy val event1CompletionInfo = DefaultParticipantStateValues.completionInfo(List.empty)
  private lazy val changeId1 = event1CompletionInfo.changeId
  private lazy val changeId1Hash = ChangeIdHash(changeId1)
  private lazy val event2CompletionInfo = DefaultParticipantStateValues
    .completionInfo(List.empty, commandId = DefaultDamlValues.commandId(3), submissionId = None)
  private lazy val changeId2 = event2CompletionInfo.changeId
  private lazy val changeId2Hash = ChangeIdHash(changeId2)
  private lazy val messageUuid = new UUID(10, 10)

  class Fixture(
      val dedup: CommandDeduplicator,
      val store: CommandDeduplicationStore,
  )

  private implicit def toOffset(i: Long): Offset = Offset.tryFromLong(i)

  private def mk(lowerBound: CantonTimestamp = CantonTimestamp.MinValue): Fixture = {
    val store = new InMemoryCommandDeduplicationStore(loggerFactory)
    val dedup =
      new CommandDeduplicatorImpl(Eval.now(store), clock, Eval.now(lowerBound), loggerFactory)
    new Fixture(dedup, store)
  }

  private def dedupOffset(longOffset: Long): DeduplicationPeriod.DeduplicationOffset =
    DeduplicationPeriod.DeduplicationOffset(Option(Offset.tryFromLong(longOffset)))

  private def mkPublicationInternal(
      longOffset: Long,
      completionInfo: CompletionInfo,
      accepted: Boolean,
      publicationTime: CantonTimestamp,
  ): PostPublishData =
    PostPublishData(
      submissionDomainId = domainId,
      publishSource = PublishSource.Local(messageUuid),
      applicationId = completionInfo.applicationId,
      commandId = completionInfo.commandId,
      actAs = completionInfo.actAs.toSet,
      offset = Offset.tryFromLong(longOffset),
      publicationTime = publicationTime,
      submissionId = completionInfo.submissionId,
      accepted = accepted,
      traceContext = implicitly,
    )

  private def mkRejectedPublication(
      longOffset: Long,
      completionInfo: CompletionInfo,
      publicationTime: CantonTimestamp,
  ): PostPublishData = mkPublicationInternal(longOffset, completionInfo, false, publicationTime)

  private def mkAcceptedPublication(
      longOffset: Long,
      completionInfo: CompletionInfo,
      publicationTime: CantonTimestamp,
  ): PostPublishData = mkPublicationInternal(longOffset, completionInfo, true, publicationTime)

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
          .checkDuplication(changeId2Hash, dedupOffset(100L))
          .valueOrFail("dedup 3")
      } yield {
        offset1 shouldBe dedupOffset(Offset.firstOffset.unwrap)
        offset3 shouldBe dedupOffset(Offset.firstOffset.unwrap)
      }
    }.failOnShutdown

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
    }.failOnShutdown

    "persist publication" in {
      val fix = mk()
      val offset1 = 3L
      val publicationTime1 = CantonTimestamp.ofEpochSecond(1)
      val publication1 =
        mkAcceptedPublication(offset1, event1CompletionInfo, publicationTime1)
      val offset2 = 4L
      val publicationTime2 = CantonTimestamp.ofEpochSecond(2)
      val publication2 = mkRejectedPublication(offset2, event2CompletionInfo, publicationTime2)
      val offset3 = 6L
      val publicationTime3 = CantonTimestamp.ofEpochSecond(3)
      val publication3 =
        mkRejectedPublication(offset3, event1CompletionInfo, publicationTime3)
      for {
        _ <- fix.dedup.processPublications(Seq(publication1, publication2))
        lookup1 <- valueOrFailUS(fix.store.lookup(changeId1Hash))("find event1")
        lookup2 <- valueOrFailUS(fix.store.lookup(changeId2Hash))("find event3")
        _ <- fix.dedup.processPublications(
          Seq(publication3)
        ) // update changeId1 and ignore event2
        lookup3 <- valueOrFailUS(fix.store.lookup(changeId1Hash))("find event1")
      } yield {
        val definiteAnswerEvent1 =
          DefiniteAnswerEvent(
            offset1,
            publicationTime1,
            submissionId1,
          )(TraceContext.empty)
        lookup1 shouldBe CommandDeduplicationData.tryCreate(
          changeId1,
          definiteAnswerEvent1,
          definiteAnswerEvent1.some,
        )
        val definiteAnswerEvent2 =
          DefiniteAnswerEvent(offset2, publicationTime2, None)(TraceContext.empty)
        lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId2, definiteAnswerEvent2, None)
        val definiteAnswerEvent3 =
          DefiniteAnswerEvent(
            offset3,
            publicationTime3,
            submissionId1,
          )(TraceContext.empty)
        lookup3 shouldBe CommandDeduplicationData.tryCreate(
          changeId1,
          definiteAnswerEvent3,
          definiteAnswerEvent1.some,
        )
      }
    }.failOnShutdown

    "deduplicate accepted commands" in {
      val fix = mk()
      val publicationTime = clock.now
      val offset1 = 3L
      val publication1 = mkAcceptedPublication(offset1, event1CompletionInfo, publicationTime)

      for {
        _ <- fix.dedup.processPublications(Seq(publication1))
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
          .checkDuplication(changeId2Hash, dedupOffset(offset1.toLong))
          .valueOrFail("do dedup conflict for other change ID")
      } yield {
        errorTime shouldBe AlreadyExists(
          offset1,
          accepted = true,
          submissionId1,
        )
        okTime shouldBe dedupOffset(offset1.toLong)
        errorOffset shouldBe AlreadyExists(
          offset1,
          accepted = true,
          submissionId1,
        )
        okOffset4 shouldBe dedupOffset(offset1.toLong)
        okOffset5 shouldBe dedupOffset(offset1.toLong)
        okOther shouldBe dedupOffset(Offset.firstOffset.unwrap)
      }
    }.failOnShutdown

    "not deduplicate upon a rejection" in {
      val fix = mk()
      val publicationTime1 = clock.now
      val offset1 = 3L
      val publication1 =
        mkRejectedPublication(offset1, event1CompletionInfo, publicationTime1)

      clock.advance(java.time.Duration.ofHours(1))
      val publicationTime2 = clock.now
      val offset2 = 5L
      val publication2 =
        mkAcceptedPublication(offset2, event1CompletionInfo, publicationTime2)

      clock.advance(java.time.Duration.ofHours(1))
      val offset3 = 10L
      val publication3 =
        mkRejectedPublication(offset3, event1CompletionInfo, publicationTime2)

      for {
        _ <- fix.dedup.processPublications(Seq(publication1))
        () = clock.advance(java.time.Duration.ofDays(1))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .valueOrFail("no dedup conflict by time")
        okOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1.toLong - 1))
          .valueOrFail("no dedup conflict by offset")
        _ <- fix.dedup.processPublications(Seq(publication2, publication3))
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
          .checkDuplication(changeId1Hash, dedupOffset(offset2.toLong))
          .valueOrFail("no dedup conflict by offset at accept")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset2.toLong - 1))
          .leftOrFail("dedup conflict by offset before accept")
      } yield {
        okTime shouldBe dedupOffset(Offset.firstOffset.unwrap)
        okOffset shouldBe dedupOffset(Offset.firstOffset.unwrap)
        okTimeAfter shouldBe dedupOffset(offset2.toLong)
        errorTime shouldBe AlreadyExists(
          offset2,
          accepted = true,
          submissionId1,
        )
        okOffsetAfter shouldBe dedupOffset(offset2.toLong)
        errorOffset shouldBe AlreadyExists(
          offset2,
          accepted = true,
          submissionId1,
        )
      }
    }.failOnShutdown

    "check pruning bound" in {
      val fix = mk()
      val pruningTime = clock.now
      val pruningOffset = 30L
      clock.advance(java.time.Duration.ofDays(1))
      for {
        _ <- fix.store.prune(pruningOffset, pruningTime)
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
    }.failOnShutdown

    "ignore pruning bound when there is an acceptance" in {
      val fix = mk()
      val publicationTime1 = clock.now
      val offset1 = 5L
      val publication1 =
        mkAcceptedPublication(offset1, event1CompletionInfo, publicationTime1)

      clock.advance(java.time.Duration.ofHours(1))
      val publicationTime2 = clock.now
      val offset2 = 10L
      val publication2 =
        mkRejectedPublication(offset2, event1CompletionInfo, publicationTime2)

      for {
        _ <- fix.dedup.processPublications(Seq(publication1))
        _ <- fix.dedup.processPublications(Seq(publication2))
        _ = clock.advance(java.time.Duration.ofHours(23))
        // This shouldn't prune the entry because the most recent definite answer is after the pruning offset
        // So we can still specify dedup periods that start before the pruning
        pruningOffset = offset1.toLong + 2L
        _ <- fix.store.prune(pruningOffset, publicationTime1.plusSeconds(10))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("no dedup conflict on time")
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup conflict on time")
        okOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1.toLong))
          .valueOrFail("no dedup conflict on offset")
        errorOffset <- fix.dedup
          .checkDuplication(changeId1Hash, dedupOffset(offset1.toLong - 1))
          .leftOrFail("dedup conflict on offset")
        otherTime <- fix.dedup
          .checkDuplication(changeId2Hash, dedupTimeAlmost1Day)
          .leftOrFail("dedup time conflict on other")
        otherOffset <- fix.dedup
          .checkDuplication(changeId2Hash, dedupOffset(pruningOffset - 1L))
          .leftOrFail("dedup time conflict on pre-pruning offset")
      } yield {
        okTime shouldBe dedupOffset(offset1.toLong)
        errorTime shouldBe AlreadyExists(offset1.toLong, accepted = true, submissionId1)
        okOffset shouldBe dedupOffset(offset1.toLong)
        errorOffset shouldBe AlreadyExists(offset1.toLong, accepted = true, submissionId1)
        otherTime shouldBe DeduplicationPeriodTooEarly(
          dedupTimeAlmost1Day,
          dedupOffset(pruningOffset),
        )
        otherOffset shouldBe DeduplicationPeriodTooEarly(
          dedupOffset(pruningOffset - 1),
          dedupOffset(pruningOffset),
        )
      }
    }.failOnShutdown

    "consider the publication time bound" in {
      val publicationTime1 = clock.now
      val offset1 = 5L
      val publication1 =
        mkAcceptedPublication(offset1, event1CompletionInfo, publicationTime1)

      val lowerBound = publicationTime1.add(java.time.Duration.ofDays(1))
      val fix = mk(lowerBound)

      clock.advance(java.time.Duration.ofHours(23))

      for {
        _ <- fix.dedup.processPublications(Seq(publication1))
        okTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTimeAlmost1Day)
          .valueOrFail("no dedup conflict on time")
        errorTime <- fix.dedup
          .checkDuplication(changeId1Hash, dedupTime1Day)
          .leftOrFail("dedup conflict on time")
      } yield {
        okTime shouldBe dedupOffset(offset1.toLong)
        errorTime shouldBe AlreadyExists(offset1.toLong, accepted = true, submissionId1)
      }
    }.failOnShutdown
  }
}

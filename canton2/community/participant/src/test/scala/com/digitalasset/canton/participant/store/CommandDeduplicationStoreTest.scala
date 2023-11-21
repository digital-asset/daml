// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.ChangeIdHash
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.{ApplicationId, BaseTest, CommandId, DefaultDamlValues, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

trait CommandDeduplicationStoreTest extends BaseTest { this: AsyncWordSpec =>

  private lazy val applicationId1 = ApplicationId.assertFromString("applicationId-1")
  private lazy val applicationId2 = ApplicationId.assertFromString("applicationId-2")
  private lazy val commandId1 = CommandId.assertFromString("commandId1")
  private lazy val commandId2 = CommandId.assertFromString("commandId2")
  private lazy val alice = LfPartyId.assertFromString("Alice")
  private lazy val bob = LfPartyId.assertFromString("Bob")

  private lazy val changeId1a = ChangeId(applicationId1.unwrap, commandId1.unwrap, Set(alice))
  private lazy val changeId1ab = ChangeId(applicationId1.unwrap, commandId1.unwrap, Set(alice, bob))
  private lazy val changeId2 = ChangeId(applicationId2.unwrap, commandId2.unwrap, Set(alice))

  private lazy val answer1 = DefiniteAnswerEvent(
    GlobalOffset.tryFromLong(1),
    CantonTimestamp.ofEpochSecond(1),
    DefaultDamlValues.submissionId(1).some,
  )(
    TraceContext.withNewTraceContext(Predef.identity)
  )
  private lazy val answer2 = DefiniteAnswerEvent(
    GlobalOffset.tryFromLong(2),
    CantonTimestamp.ofEpochSecond(2),
    DefaultDamlValues.submissionId(2).some,
  )(
    TraceContext.withNewTraceContext(Predef.identity)
  )
  private lazy val answer3 =
    DefiniteAnswerEvent(GlobalOffset.tryFromLong(3), CantonTimestamp.ofEpochSecond(3), None)(
      TraceContext.withNewTraceContext(Predef.identity)
    )

  protected def commandDeduplicationStore(mk: () => CommandDeduplicationStore): Unit = {

    "empty" should {
      "return None" in {
        val store = mk()
        for {
          lookup <- store.lookup(ChangeIdHash(changeId1a)).value
          pruning <- store.latestPruning().value
        } yield {
          lookup shouldBe None
          pruning shouldBe None
        }
      }
    }

    "storeDefiniteAnswers" should {
      "store rejections" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, false),
              (changeId2, answer2, false),
            )
          )
          lookup1a <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup 1a")
          lookup2 <- valueOrFail(store.lookup(ChangeIdHash(changeId2)))("lookup 2")
          lookupOther <- store.lookup(ChangeIdHash(changeId1ab)).value
        } yield {
          lookup1a shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer1, None)
          lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId2, answer2, None)
          lookupOther shouldBe None
        }
      }

      "store acceptances" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, true),
              (changeId2, answer2, true),
            )
          )
          lookup1a <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup 1a")
          lookup2 <- valueOrFail(store.lookup(ChangeIdHash(changeId2)))("lookup 2")
          lookupOther <- store.lookup(ChangeIdHash(changeId1ab)).value
        } yield {
          lookup1a shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer1, answer1.some)
          lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId2, answer2, answer2.some)
          lookupOther shouldBe None
        }
      }

      "idempotent store of acceptances" in {
        val store = mk()
        val answer1WithDifferentTC =
          answer1.copy()(traceContext = TraceContext.withNewTraceContext(Predef.identity))
        for {
          _ <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, true)
            )
          )
          _ <- loggerFactory.assertLogsUnorderedOptional(
            store.storeDefiniteAnswers(
              Seq(
                (
                  changeId1a,
                  answer1WithDifferentTC,
                  true,
                )
              )
            ),
            (LogEntryOptionality.Optional -> (_.warningMessage should include(
              "Looked and found expected command deduplication data"
            ))),
          )
        } yield {
          succeed
        }
      }

      "update an acceptance" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, true),
              (changeId2, answer1, true),
            )
          )
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer2, true), // update with an acceptance
              (changeId2, answer2, false), // update with a rejection
            )
          )
          lookup1a <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup 1a")
          lookup2 <- valueOrFail(store.lookup(ChangeIdHash(changeId2)))("lookup 2")
        } yield {
          lookup1a shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer2, answer2.some)
          lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId2, answer2, answer1.some)
        }
      }

      "update a rejection" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswer(changeId1a, answer1, accepted = false)
          () <- store.storeDefiniteAnswer(
            changeId1a,
            answer2,
            accepted = false,
          ) // update with a rejection
          lookupR <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup rejection")
          () <- store.storeDefiniteAnswer(
            changeId1a,
            answer3,
            accepted = true,
          ) // update with an acceptance
          lookupA <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup acceptance")
        } yield {
          lookupR shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer2, None)
          lookupA shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer3, answer3.some)
        }
      }

      "several updates in one batch" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, true),
              (changeId1ab, answer1, true),
              (changeId1a, answer2, false), // Overwrite with rejection
              (changeId1ab, answer3, true), // Overwrite with acceptance
            )
          )
          lookup1a <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup 1a")
          lookup1ab <- valueOrFail(store.lookup(ChangeIdHash(changeId1ab)))("lookup 1ab")
        } yield {
          lookup1a shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer2, answer1.some)
          lookup1ab shouldBe CommandDeduplicationData.tryCreate(changeId1ab, answer3, answer3.some)
        }
      }

      "not overwrite later offsets" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswer(changeId1a, answer1, accepted = true)
          () <- store.storeDefiniteAnswer(changeId1a, answer3, accepted = false)
          () <- MonadUtil.sequentialTraverse_(Seq(false, true)) { accept =>
            loggerFactory.assertThrowsAndLogsAsync[IllegalArgumentException](
              store.storeDefiniteAnswer(changeId1a, answer2, accepted = accept),
              _.getMessage should include(
                s"Cannot update command deduplication data for ${ChangeIdHash(
                    changeId1a
                  )} from offset ${answer3.offset} to offset ${answer2.offset}"
              ),
              _.errorMessage should include(ErrorUtil.internalErrorMessage),
            )
          }
          lookup <- valueOrFail(store.lookup(ChangeIdHash(changeId1a)))("lookup acceptance")
        } yield {
          lookup shouldBe CommandDeduplicationData.tryCreate(changeId1a, answer3, answer1.some)
        }
      }
    }

    "pruning" should {
      "update the pruning data" in {
        val store = mk()
        for {
          empty <- store.latestPruning().value
          () <- store.prune(answer1.offset, answer1.publicationTime)
          first <- valueOrFail(store.latestPruning())("first pruning lookup")
          () <- store.prune(answer2.offset, answer2.publicationTime)
          second <- valueOrFail(store.latestPruning())("second pruning lookup")
        } yield {
          empty shouldBe None
          first shouldBe OffsetAndPublicationTime(answer1.offset, answer1.publicationTime)
          second shouldBe OffsetAndPublicationTime(answer2.offset, answer2.publicationTime)
        }
      }

      "only advance the pruning data" in {
        val store = mk()
        for {
          () <- store.prune(answer2.offset, answer2.publicationTime)
          baseline <- valueOrFail(store.latestPruning())("baseline pruning lookup")
          () <- store.prune(answer1.offset, answer1.publicationTime)
          tooLow <- valueOrFail(store.latestPruning())("tooLow pruning lookup")
          () <- store.prune(answer3.offset, answer1.publicationTime)
          publicationTimeTooLow <- valueOrFail(store.latestPruning())(
            "publicationTimeTooLow pruning lookup"
          )
          () <- store.prune(answer1.offset, answer3.publicationTime)
          offsetTooLow <- valueOrFail(store.latestPruning())("offsetTooLow pruning lookup")
        } yield {
          baseline shouldBe OffsetAndPublicationTime(answer2.offset, answer2.publicationTime)
          tooLow shouldBe OffsetAndPublicationTime(answer2.offset, answer2.publicationTime)
          publicationTimeTooLow shouldBe OffsetAndPublicationTime(
            answer3.offset,
            answer2.publicationTime,
          )
          offsetTooLow shouldBe OffsetAndPublicationTime(answer3.offset, answer3.publicationTime)
        }
      }

      "remove by latest definite answer offset" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswers(
            Seq(
              (changeId1a, answer1, false),
              (changeId1ab, answer2, true),
              (changeId2, answer3, false),
            )
          )
          () <- store.prune(answer2.offset, CantonTimestamp.MaxValue)
          lookup1a <- store.lookup(ChangeIdHash(changeId1a)).value
          lookup1ab <- store.lookup(ChangeIdHash(changeId1ab)).value
          lookup2 <- store.lookup(ChangeIdHash(changeId2)).value
          () <- store.prune(answer3.offset, CantonTimestamp.MinValue)
          lookup2e <- store.lookup(ChangeIdHash(changeId2)).value
        } yield {
          lookup1a shouldBe None
          lookup1ab shouldBe None
          lookup2 shouldBe CommandDeduplicationData.tryCreate(changeId2, answer3, None).some
          lookup2e shouldBe None
        }
      }

      "keep outdated acceptances" in {
        val store = mk()
        for {
          () <- store.storeDefiniteAnswer(changeId1a, answer1, accepted = true)
          () <- store.storeDefiniteAnswer(changeId1a, answer3, accepted = false)
          () <- store.prune(answer2.offset, answer2.publicationTime)
          lookup1a <- store.lookup(ChangeIdHash(changeId1a)).value
        } yield {
          lookup1a shouldBe CommandDeduplicationData
            .tryCreate(changeId1a, answer3, answer1.some)
            .some
        }
      }
    }
  }
}

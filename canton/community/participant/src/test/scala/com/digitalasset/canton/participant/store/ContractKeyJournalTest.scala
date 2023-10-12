// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.lf.value.Value.ValueInt64
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.ContractKeyJournal.{
  Assigned,
  ContractKeyState,
  InconsistentKeyAllocationStatus,
  Unassigned,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{LfTransactionBuilder, MonadUtil}
import com.digitalasset.canton.{BaseTest, RequestCounter, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.util.Random

@nowarn("msg=match may not be exhaustive")
trait ContractKeyJournalTest extends PrunableByTimeTest {
  this: AsyncWordSpecLike & BaseTest & TestMetrics =>
  import ContractKeyJournalTest.*

  def contractKeyJournal(mkCkj: ExecutionContext => ContractKeyJournal): Unit = {
    def mk(): ContractKeyJournal = mkCkj(executionContext)

    behave like prunableByTime(mkCkj)

    val keys @ Seq(key0, key1, key2, key3) = (0L to 3L).map(globalKey)
    val otherKey = globalKey(-1L)
    val keys012 = List(key0, key1, key2)

    val rc = RequestCounter(0)
    val ts = CantonTimestamp.assertFromInstant(Instant.parse("2002-02-20T10:00:00.00Z"))
    val toc = TimeOfChange(rc, ts)
    val toc1 = TimeOfChange(rc + 1L, ts)
    val toc2 = TimeOfChange(rc + 2L, ts.plusMillis(1))
    val toc3 = TimeOfChange(rc, ts.plusMillis(2))

    def keysWithStatus(toc: TimeOfChange) = keys.zipWithIndex.map { case (key, index) =>
      key -> ((if (index % 2 == 0) Assigned else Unassigned), toc)
    }.toMap

    "fetchKeyStates" should {
      "return Unknown on an empty contract key journal" in {
        val ckj = mk()
        for {
          found <- ckj.fetchStates(keys)
        } yield {
          found shouldBe Map.empty[LfGlobalKey, ContractKeyState]
        }
      }

      "return the latest contract key state" in {
        val ckj = mk()
        val updates = Seq(
          Map(key0 -> Assigned, key1 -> Assigned) -> toc,
          Map(key1 -> Unassigned, key2 -> Assigned) -> toc1,
          Map(key2 -> Unassigned, key3 -> Unassigned) -> toc2,
          Map(key3 -> Assigned) -> toc3,
        )
        val rand = new Random(1234567890L)
        def moveToc(toc: TimeOfChange, offset: Int): TimeOfChange =
          TimeOfChange(
            toc.rc + offset * 100,
            toc.timestamp.plusSeconds(offset.toLong),
          )

        MonadUtil
          .sequentialTraverse_(0 to 20) { iteration =>
            val shuffledUpdates = rand.shuffle(updates)
            for {
              _ <- MonadUtil.sequentialTraverse_(shuffledUpdates) { case (upd, updToc) =>
                val updTocMoved = moveToc(updToc, iteration)
                valueOrFail(ckj.addKeyStateUpdates(upd.view.mapValues(_ -> updTocMoved).toMap))(
                  show"Iteration $iteration: Add updates $upd at $updTocMoved"
                )
              }
              query <- ckj.fetchStates(keys)
            } yield {
              query shouldBe Map(
                key0 -> ContractKeyState(Assigned, moveToc(toc, iteration)),
                // RequestCounter is used to order updates with same timestamp
                key1 -> ContractKeyState(Unassigned, moveToc(toc1, iteration)),
                // Timestamp and request counter increase with second update
                key2 -> ContractKeyState(Unassigned, moveToc(toc2, iteration)),
                // Timestamp beats request counter w.r.t. ordering
                key3 -> ContractKeyState(Assigned, moveToc(toc3, iteration)),
              )
            }
          }
          .map(_ => succeed)
      }
    }

    "addKeyStateUpdates" should {
      "store the updates" in {
        val ckj = mk()
        for {
          _ <- valueOrFail(ckj.addKeyStateUpdates(keysWithStatus(toc)))("add keys to empty journal")
          found <- ckj.fetchStates(keys)
          notFound <- ckj.fetchStates(Seq(otherKey))
        } yield {
          found shouldBe keysWithStatus(toc).fmap { case (status, toc) =>
            ContractKeyState(status, toc)
          }
          notFound shouldBe Map.empty[LfGlobalKey, ContractKeyState]
        }
      }

      "be idempotent" in {
        val ckj = mk()
        for {
          _ <- valueOrFail(ckj.addKeyStateUpdates(keysWithStatus(toc)))("add keys to empty journal")
          _ <- valueOrFail(ckj.addKeyStateUpdates(keysWithStatus(toc)))(
            "add same updates to journal"
          )
          found <- ckj.fetchStates(keys)
        } yield {
          found shouldBe keysWithStatus(toc).fmap { case (status, toc) =>
            ContractKeyState(status, toc)
          }
        }
      }

      "complain about conflicting updates" in {
        val ckj = mk()
        for {
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Assigned, toc), key1 -> (Unassigned, toc)))
          )(
            "add keys to empty journal"
          )
          err1 <- leftOrFail(ckj.addKeyStateUpdates(Map(key1 -> (Assigned, toc))))(
            "add single conflicting key"
          )
          find1 <- ckj.fetchStates(Seq(key1))
          err0 <- leftOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Unassigned, toc), key2 -> (Assigned, toc)))
          )("one conflicting key and one conflict-free key")
          find2 <- ckj.fetchStates(Seq(key0, key2))
        } yield {
          err1 shouldBe InconsistentKeyAllocationStatus(key1, toc, Unassigned, Assigned)
          find1 shouldBe Map(key1 -> ContractKeyState(Unassigned, toc))
          err0 shouldBe InconsistentKeyAllocationStatus(key0, toc, Assigned, Unassigned)
          // partial writes are OK in case of an error
          find2 should (equal(Map(key0 -> ContractKeyState(Assigned, toc))) or
            be(
              Map(key0 -> ContractKeyState(Assigned, toc), key2 -> ContractKeyState(Assigned, toc))
            ))
        }
      }

      "allow empty updates" in {
        val ckj = mk()
        for {
          _ <- valueOrFail(ckj.addKeyStateUpdates(Map.empty))("empty update")
        } yield succeed
      }
    }

    "prune" should {
      "remove the obsolete updates" in {
        val ckj = mk()
        for {
          _ <- ckj.prune(CantonTimestamp.Epoch)
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Assigned, toc), key1 -> (Assigned, toc)))
          )(
            s"Add keys at $toc"
          )
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key1 -> (Assigned, toc1), key2 -> (Assigned, toc1)))
          )(
            s"Add keys at $toc1"
          )
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Assigned, toc2), key2 -> (Assigned, toc2)))
          )(
            s"Add keys at $toc2"
          )
          _ <- ckj.prune(toc1.timestamp)
          fetch1 <- ckj.fetchStates(keys012)
          count1 <- keys012.parTraverse(ckj.countUpdates)
          _ <- valueOrFail(ckj.addKeyStateUpdates(Map(key1 -> (Unassigned, toc2))))(
            s"Add key $key1 at $toc2"
          )
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Unassigned, toc3), key2 -> (Assigned, toc3)))
          )(
            s"Add keys at $toc3"
          )
          _ <- ckj.prune(toc3.timestamp)
          fetch2 <- ckj.fetchStates(keys012)
          count2 <- keys012.parTraverse(ckj.countUpdates)
        } yield {
          fetch1 shouldBe Map(
            key0 -> ContractKeyState(Assigned, toc2),
            key1 -> ContractKeyState(Assigned, toc1), // Keep last update before pruning timestamp
            key2 -> ContractKeyState(Assigned, toc2),
          )
          count1 shouldBe List(2, 1, 2)
          fetch2 shouldBe Map(
            key2 -> ContractKeyState(Assigned, toc3)
          )
          count2 shouldBe List(0, 0, 1)
        }
      }
    }

    "deleteSince" should {
      "remove the updates at or after the criterion" in {
        val ckj = mk()
        for {
          _ <- valueOrFail(ckj.deleteSince(toc))(s"Delete since $toc in the empty journal")
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Unassigned, toc), key1 -> (Assigned, toc)))
          )(
            s"Add keys at $toc"
          )
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key1 -> (Assigned, toc1), key2 -> (Assigned, toc1)))
          )(
            s"Add keys at $toc1"
          )
          _ <- valueOrFail(
            ckj.addKeyStateUpdates(Map(key0 -> (Assigned, toc2), key2 -> (Assigned, toc2)))
          )(
            s"Add keys at $toc2"
          )
          _ <- valueOrFail(ckj.deleteSince(toc1))(s"Delete since $toc1")
          fetch1 <- ckj.fetchStates(keys012)
          count1 <- keys012.parTraverse(ckj.countUpdates)
          _ <- valueOrFail(ckj.deleteSince(toc))(s"Delete since $toc")
          fetch2 <- ckj.fetchStates(keys012)
        } yield {
          fetch1 shouldBe Map(
            key0 -> ContractKeyState(Unassigned, toc),
            key1 -> ContractKeyState(Assigned, toc),
          )
          count1 shouldBe List(1, 1, 0)
          fetch2 shouldBe Map.empty[LfGlobalKey, ContractKeyState]
        }
      }
    }
  }
}

object ContractKeyJournalTest {
  def globalKey(keyIndex: Long): LfGlobalKey =
    LfGlobalKey.assertBuild(LfTransactionBuilder.defaultTemplateId, ValueInt64(keyIndex))
}

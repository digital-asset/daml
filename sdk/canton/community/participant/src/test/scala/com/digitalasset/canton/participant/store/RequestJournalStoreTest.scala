// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.HasCloseContext
import com.digitalasset.canton.participant.admin.repair.RepairContext
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.*
import com.digitalasset.canton.store.{CursorPrehead, CursorPreheadStoreTest}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, RequestCounter}
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.Future

trait RequestJournalStoreTest extends CursorPreheadStoreTest {
  this: AsyncWordSpecLike with BaseTest with HasCloseContext =>

  def requestJournalStore(mk: () => RequestJournalStore): Unit = {
    val rc = RequestCounter(0)
    val ts = CantonTimestamp.Epoch
    def tsWithSecs(secs: Long) = CantonTimestamp.ofEpochSecond(secs)
    val commitTime = CantonTimestamp.ofEpochSecond(1000)
    val requests = List(
      (rc, Pending, tsWithSecs(1)),
      (rc + 2, Clean, tsWithSecs(3)),
      (rc + 4, Pending, tsWithSecs(5)),
    )
    def setupRequests(store: RequestJournalStore): Future[List[Unit]] = Future.traverse(requests) {
      case (reqC, state, timestamp) =>
        val data = state match {
          case Clean => RequestData.clean(reqC, timestamp, commitTime)
          case Pending => RequestData.initial(reqC, timestamp)
        }
        store.insert(data)
    }

    "insert and retrieve request" in {
      val store = mk()
      val data = RequestData(rc, Pending, ts)
      for {
        _ <- store.insert(data)
        result <- store.query(rc).value
      } yield result shouldBe Some(data)
    }

    "retrieve first request with commit time bound" should {
      "return None for the empty store" in {
        val store = mk()
        for {
          result <- store.firstRequestWithCommitTimeAfter(ts)
        } yield result shouldBe None
      }

      "find the first request by request counter whose commit time is after" in {
        val store = mk()
        val data0 = RequestData.initial(rc, ts)
        val data1 = RequestData.clean(rc + 1L, ts, ts.plusSeconds(10))
        val data2 = RequestData.clean(rc + 2L, ts.plusSeconds(2), ts.plusSeconds(4))
        val data3 = RequestData.clean(rc + 3L, ts.plusSeconds(2), ts.plusSeconds(5))
        val inserts = List(data0, data1, data2, data3)
        for {
          _ <- inserts.parTraverse(store.insert)
          find5 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(5))
          find1 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(1))
          find10 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(10))
          _ <- valueOrFail(store.replace(rc, ts, Clean, Some(ts.plusSeconds(10))))(
            s"replace $rc from $Pending to $Clean"
          )
          find7 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(7))
        } yield {
          find5 shouldBe Some(data1) // time bound is exclusive
          find1 shouldBe Some(data1) // find the first by request counter, not by the commit time
          find10 shouldBe None // time bound is exclusive

          find7 shouldBe Some(
            RequestData.clean(rc, ts, ts.plusSeconds(10))
          ) // the request counter decides draws
        }
      }
    }

    "inserting is idempotent" in {
      val store = mk()
      val data = RequestData(rc, Pending, ts, Some(RepairContext.tryCreate("repair-trace-context")))
      for {
        _ <- store.insert(data)
        _ <- store.insert(data)
        result <- store.query(rc).value
      } yield result shouldBe Some(data)
    }

    "inserting fails if inserting conflicting requests" in {
      val store = mk()
      val data = RequestData(rc, Pending, ts)
      val data2 = RequestData(rc, Pending, ts.plusSeconds(10))
      for {
        _ <- store.insert(data)
        _ <- loggerFactory.suppressWarningsAndErrors(store.insert(data2).failed)
      } yield succeed
    }

    "inserting repair request preserves repair context" in {
      val store = mk()
      val firstRepair = RepairContext.tryCreate("first repair")
      val secondRepair = RepairContext.tryCreate("second repair")
      val data0 = RequestData(rc, Pending, ts)
      val data1 = RequestData.clean(rc + 1L, ts, ts, Some(firstRepair))
      val data2 = RequestData.clean(rc + 2L, ts, ts, Some(secondRepair))
      for {
        _regularRequest <- store.insert(data0)
        _regularResult <- valueOrFail(store.replace(rc, ts, Clean, Some(commitTime)))(
          s"relace $rc from $Pending to $Clean"
        )
        _firstRepair <- store.insert(data1)
        _secondRepair <- store.insert(data2)
        firstRepairResult <- store.query(rc + 1L).value
        secondRepairResult <- store.query(rc + 2L).value
      } yield {
        firstRepairResult shouldBe Some(data1)
        secondRepairResult shouldBe Some(data2)
      }
    }

    "replace state" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        _ <- valueOrFail(store.replace(rc, ts, Clean, Some(CantonTimestamp.Epoch)))("replace")
        result <- valueOrFail(store.query(rc))("query")
      } yield result shouldBe RequestData.clean(rc, ts, CantonTimestamp.Epoch)
    }

    "replace is idempotent" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        _ <- valueOrFail(store.replace(rc, ts, Clean, Some(CantonTimestamp.Epoch)))("replace")
        _ <- valueOrFail(store.replace(rc, ts, Clean, Some(CantonTimestamp.Epoch)))("replace again")
        result <- valueOrFail(store.query(rc))("query")
      } yield result shouldBe RequestData.clean(rc, ts, CantonTimestamp.Epoch)
    }

    "not replace if given timestamp does not match stored timestamp" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        replaced <- leftOrFail(
          store.replace(rc, ts.plusSeconds(1), Clean, Some(ts.plusSeconds(2)))
        )(
          "replace"
        )
        result <- valueOrFail(store.query(rc))("query")
      } yield {
        replaced shouldBe InconsistentRequestTimestamp(rc, ts, ts.plusSeconds(1))
        result shouldBe RequestData(rc, Pending, ts)
      }
    }

    "error if trying to replace the state of a non existing request" in {
      val store = mk()
      for {
        replaced <- leftOrFail(store.replace(rc, ts, Clean, Some(CantonTimestamp.Epoch)))("replace")
      } yield replaced shouldBe UnknownRequestCounter(rc)
    }

    "not replace if the commit time is before the request time" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        replaced <- leftOrFail(store.replace(rc, ts, Clean, Some(ts.plusSeconds(-1))))(
          "replace"
        )
        result <- valueOrFail(store.query(rc))("query")
      } yield {
        replaced shouldBe CommitTimeBeforeRequestTime(rc, ts, ts.plusSeconds(-1))
        result shouldBe RequestData(rc, Pending, ts)
      }
    }

    "size should count requests in time interval" in {
      val store = mk()
      for {
        _ <- setupRequests(store)
        totalCount <- store.size()
        countUntilTs <- store.size(end = Some(tsWithSecs(4)))
        countFromTs <- store.size(start = tsWithSecs(4))
        count45 <- store.size(tsWithSecs(4), Some(tsWithSecs(5)))
        count56 <- store.size(tsWithSecs(5), Some(tsWithSecs(6)))
      } yield {
        totalCount shouldBe 3
        countUntilTs shouldBe 2
        countFromTs shouldBe 1
        count45 shouldBe 1
        count56 shouldBe 1
      }
    }

    "prune all requests before given timestamp without checks" in {
      val store = mk()
      for {
        _ <- setupRequests(store)
        _ <- store.pruneInternal(tsWithSecs(4))
        totalCount <- store.size()
      } yield {
        totalCount shouldBe 1
      }
    }

    "prune all requests before and including given timestamp without checks" in {
      val store = mk()
      for {
        _ <- setupRequests(store)
        _ <- store.pruneInternal(tsWithSecs(3))
        totalCount <- store.size()
      } yield {
        totalCount shouldBe 1
      }
    }

    def setupPruning(store: RequestJournalStore): Future[Unit] = {
      for {
        _ <- store.insert(RequestData.clean(rc, CantonTimestamp.ofEpochSecond(1), commitTime))
        _ <- store.insert(RequestData.clean(rc + 1, CantonTimestamp.ofEpochSecond(2), commitTime))
        _ <- store.insert(RequestData.clean(rc + 2, CantonTimestamp.ofEpochSecond(3), commitTime))
        _ <- store.insert(RequestData(rc + 4, Pending, CantonTimestamp.ofEpochSecond(5)))
        _ <- store.advancePreheadCleanTo(CursorPrehead(rc + 2L, CantonTimestamp.ofEpochSecond(3)))
      } yield ()
    }

    "prune when given a sensible timestamp" in {
      val store = mk()
      for {
        _ <- setupPruning(store)
        _ <- store.prune(CantonTimestamp.ofEpochSecond(2), bypassAllSanityChecks = false)
        queryInit <- store.query(rc).value
        queryInserted <- store.query(rc + 1).value
      } yield {
        queryInit shouldBe None
        queryInserted shouldBe None
      }
    }

    "fail to prune without a clean cursor" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData.initial(rc, ts))
        _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
          store.prune(ts, bypassAllSanityChecks = false),
          _.getMessage shouldBe "Attempted to prune a journal with no clean timestamps",
        )
      } yield succeed
    }

    "fail to prune at a timestamp no earlier than the one associated to the clean cursor" in {
      val store = mk()
      for {
        _ <- setupPruning(store)
        _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
          store.prune(CantonTimestamp.ofEpochSecond(3), bypassAllSanityChecks = false),
          _.getMessage shouldBe "Attempted to prune at timestamp 1970-01-01T00:00:03Z which is not earlier than 1970-01-01T00:00:03Z associated with the clean head",
        )
      } yield succeed
    }

    "prune succeeds when bypassAllSanityChecks is set even when timestamp not earlier than the one associated to the clean cursor" in {
      val store = mk()
      val ts = CantonTimestamp.ofEpochSecond(3)
      for {
        _ <- setupPruning(store)
        _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
          store.prune(ts, bypassAllSanityChecks = false),
          _.getMessage should include(ts.toString),
        )
        _ <- store.prune(ts, bypassAllSanityChecks = true)
        queryInit <- store.query(rc).value
        queryInserted <- store.query(rc + 1).value
      } yield {
        queryInit shouldBe None
        queryInserted shouldBe None
      }
    }

    "clean prehead" should {
      behave like (cursorPreheadStore(() => mk().cleanPreheadStore, RequestCounter.apply))
    }

    "deleteSince" should {
      "remove all requests from the given counter on" in {
        val store = mk()
        val cursorHead = CursorPrehead(RequestCounter(1), tsWithSecs(2))
        for {
          _ <- setupRequests(store)
          _ <- store.advancePreheadCleanTo(cursorHead)
          _ <- store.deleteSince(RequestCounter(2))
          result0 <- valueOrFail(store.query(RequestCounter(0)))("Request 0 is retained")
          result2 <- store.query(RequestCounter(2)).value
          clean <- store.preheadClean
          _ <- store.insert(RequestData(RequestCounter(2), Pending, tsWithSecs(10)))
        } yield {
          result0 shouldBe RequestData(rc, Pending, tsWithSecs(1))
          result2 shouldBe None
          clean shouldBe Some(cursorHead)
        }
      }

      "remove all requests even if there is no request for the counter" in {
        val store = mk()
        for {
          _ <- store.insert(RequestData(RequestCounter(-3), Pending, tsWithSecs(-1)))
          _ <- store.deleteSince(RequestCounter(-5))
          result <- store.query(RequestCounter(-3)).value
          clean <- store.preheadClean
        } yield {
          result shouldBe None
          clean shouldBe None
        }
      }

      "tolerate bounds above what has been stored" in {
        val store = mk()
        for {
          _ <- store.insert(RequestData(RequestCounter(0), Pending, tsWithSecs(0)))
          _ <- store.deleteSince(RequestCounter(1))
          result <- valueOrFail(store.query(RequestCounter(0)))("Lookup request 0")
        } yield {
          result shouldBe RequestData(RequestCounter(0), Pending, tsWithSecs(0))
        }
      }
    }

    "repairRequests" should {
      "return the repair requests in ascending order" in {
        val store = mk()
        val requests = List(
          RequestData(RequestCounter(0), Pending, tsWithSecs(0)),
          RequestData(
            RequestCounter(1),
            Pending,
            tsWithSecs(0),
            repairContext = Some(RepairContext.tryCreate("repair1")),
          ),
          RequestData(
            RequestCounter(2),
            Pending,
            tsWithSecs(0),
            repairContext = Some(RepairContext.tryCreate("repair2")),
          ),
          RequestData(RequestCounter(3), Pending, tsWithSecs(3)),
          RequestData(
            RequestCounter(4),
            Pending,
            tsWithSecs(4),
            repairContext = Some(RepairContext.tryCreate("repair3")),
          ),
          RequestData(RequestCounter(6), Pending, tsWithSecs(5)),
        )
        for {
          empty <- store.repairRequests(RequestCounter.Genesis)
          _ <- requests.parTraverse_(store.insert)
          repair1 <- store.repairRequests(RequestCounter(1))
          repair2 <- store.repairRequests(RequestCounter(2))
          repair4 <- store.repairRequests(RequestCounter(4))
          repair6 <- store.repairRequests(RequestCounter(6))
        } yield {
          empty shouldBe Seq.empty
          repair1 shouldBe Seq(requests(1), requests(2), requests(4))
          repair2 shouldBe Seq(requests(2), requests(4))
          repair4 shouldBe Seq(requests(4))
          repair6 shouldBe Seq.empty
        }
      }
    }

    "totalDirtyRequests should count dirty requests" in {
      val store = mk()
      for {
        _ <- setupRequests(store)
        initialDirtyRequests <- store.totalDirtyRequests()
        // transition one of the request to the clean state and expect total dirty requests to be decreased by 1
        _ <- valueOrFail(store.replace(rc, tsWithSecs(1), Clean, Some(tsWithSecs(8))))(
          "changing one request to the clean state"
        )
        totalDirtyRequests <- store.totalDirtyRequests()
      } yield {
        initialDirtyRequests shouldBe 2
        totalDirtyRequests shouldBe 1
      }
    }
  }
}

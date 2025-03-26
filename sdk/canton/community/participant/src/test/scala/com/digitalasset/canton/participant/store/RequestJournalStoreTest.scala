// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.*
import com.digitalasset.canton.participant.util.TimeOfRequest
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, FailOnShutdown, RequestCounter}
import org.scalatest.wordspec.AsyncWordSpecLike

trait RequestJournalStoreTest extends FailOnShutdown {
  this: AsyncWordSpecLike & BaseTest & HasCloseContext =>

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
    def setupRequests(store: RequestJournalStore): FutureUnlessShutdown[Unit] = MonadUtil
      .sequentialTraverse(requests) { case (reqC, state, timestamp) =>
        val data = state match {
          case Clean => RequestData.clean(reqC, timestamp, commitTime)
          case Pending => RequestData.initial(reqC, timestamp)
        }
        store.insert(data)
      }
      .map(_ => ())

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
        val data1 = RequestData.clean(rc + 1L, ts.plusSeconds(1), ts.plusSeconds(10))
        val data2 = RequestData.clean(rc + 2L, ts.plusSeconds(2), ts.plusSeconds(4))
        val data3 = RequestData.clean(rc + 3L, ts.plusSeconds(3), ts.plusSeconds(5))
        val inserts = List(data0, data1, data2, data3)
        for {
          _ <- inserts.parTraverse(store.insert)
          find5 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(5))
          find1 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(1))
          find10 <- store.firstRequestWithCommitTimeAfter(ts.plusSeconds(10))
          _ <- valueOrFail(
            store
              .replace(rc, ts, Clean, Some(ts.plusSeconds(10)))
          )(
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
      val data = RequestData(rc, Pending, ts)
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

    "inserting fails if inserting conflicting timestamps" in {
      val store = mk()
      val data = RequestData(rc, Pending, ts)
      val data2 = RequestData(rc + 1, Pending, ts)
      for {
        _ <- store.insert(data)
        _ <- loggerFactory.suppressWarningsAndErrors(store.insert(data2).failed)
      } yield succeed
    }

    "replace state" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        _ <- valueOrFail(
          store
            .replace(rc, ts, Clean, Some(CantonTimestamp.Epoch))
        )("replace")
        result <- valueOrFailUS(store.query(rc))("query")
      } yield result shouldBe RequestData.clean(rc, ts, CantonTimestamp.Epoch)
    }

    "replace is idempotent" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        _ <- valueOrFail(
          store
            .replace(rc, ts, Clean, Some(CantonTimestamp.Epoch))
        )("replace")
        _ <- valueOrFail(
          store
            .replace(rc, ts, Clean, Some(CantonTimestamp.Epoch))
        )("replace again")
        result <- valueOrFailUS(store.query(rc))("query")
      } yield result shouldBe RequestData.clean(rc, ts, CantonTimestamp.Epoch)
    }

    "not replace if given timestamp does not match stored timestamp" in {
      val store = mk()
      for {
        _ <- store.insert(RequestData(rc, Pending, ts))
        replaced <- leftOrFail(
          store
            .replace(rc, ts.plusSeconds(1), Clean, Some(ts.plusSeconds(2)))
        )(
          "replace"
        )
        result <- valueOrFailUS(store.query(rc))("query")
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
        replaced <- leftOrFail(
          store.replace(rc, ts, Clean, Some(ts.plusSeconds(-1)))
        )(
          "replace"
        )
        result <- valueOrFailUS(store.query(rc))("query")
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

    def setupPruning(store: RequestJournalStore): FutureUnlessShutdown[Unit] =
      for {
        _ <- store.insert(RequestData.clean(rc, CantonTimestamp.ofEpochSecond(1), commitTime))
        _ <- store.insert(RequestData.clean(rc + 1, CantonTimestamp.ofEpochSecond(2), commitTime))
        _ <- store.insert(RequestData.clean(rc + 2, CantonTimestamp.ofEpochSecond(3), commitTime))
        _ <- store.insert(RequestData(rc + 4, Pending, CantonTimestamp.ofEpochSecond(5)))
      } yield ()

    "prune when given a sensible timestamp" in {
      val store = mk()
      for {
        _ <- setupPruning(store)
        _ <- store.prune(CantonTimestamp.ofEpochSecond(2))
        queryInit <- store.query(rc).value
        queryInserted <- store.query(rc + 1).value
      } yield {
        queryInit shouldBe None
        queryInserted shouldBe None
      }
    }

    "purge entire request journal" in {
      val store = mk()
      for {
        _ <- setupPruning(store)
        journalSizeBeforePurge <- store.size()
        _ <- store.purge()
        journalSizeAfterPurge <- store.size()
      } yield {
        journalSizeBeforePurge shouldBe 4
        journalSizeAfterPurge shouldBe 0
      }
    }

    "deleteSince" should {
      "remove all requests from the given request timestamp on" in {
        val store = mk()
        for {
          _ <- setupRequests(store)
          _ <- store.deleteSinceRequestTimestamp(tsWithSecs(3))
          result0 <- valueOrFailUS(
            store.query(RequestCounter(0))
          )("Request 0 is retained")
          result2 <- store.query(RequestCounter(2)).value
          _ <- store.insert(RequestData(RequestCounter(2), Pending, tsWithSecs(10)))
        } yield {
          result0 shouldBe RequestData(rc, Pending, tsWithSecs(1))
          result2 shouldBe None
        }
      }

      "remove all requests even if there is no request for the timestamp" in {
        val store = mk()
        for {
          _ <- store.insert(RequestData(RequestCounter(-3), Pending, tsWithSecs(-1)))
          _ <- store.deleteSinceRequestTimestamp(tsWithSecs(-1).immediatePredecessor)
          result <- store.query(RequestCounter(-3)).value
        } yield {
          result shouldBe None
        }
      }

      "tolerate bounds above what has been stored" in {
        val store = mk()
        for {
          _ <- store.insert(RequestData(RequestCounter(0), Pending, tsWithSecs(0)))
          _ <- store.deleteSinceRequestTimestamp(tsWithSecs(0).immediateSuccessor)
          result <- valueOrFailUS(
            store.query(RequestCounter(0))
          )("Lookup request 0")
        } yield {
          result shouldBe RequestData(RequestCounter(0), Pending, tsWithSecs(0))
        }
      }
    }

    "totalDirtyRequests should count dirty requests" in {
      val store = mk()
      for {
        _ <- setupRequests(store)
        initialDirtyRequests <- store.totalDirtyRequests()
        // transition one of the request to the clean state and expect total dirty requests to be decreased by 1
        _ <- valueOrFail(
          store
            .replace(rc, tsWithSecs(1), Clean, Some(tsWithSecs(8)))
        )(
          "changing one request to the clean state"
        )
        totalDirtyRequests <- store.totalDirtyRequests()
      } yield {
        initialDirtyRequests shouldBe NonNegativeInt.tryCreate(2)
        totalDirtyRequests shouldBe NonNegativeInt.one
      }
    }

    "lastRequestCounterWithRequestTimestampBeforeOrAt" should {
      "return the last request counter before a request timestamp" in {
        val store = mk()
        for {
          _ <- setupRequests(store)
          early <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(0))
          firstAt <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(1))
          firstAfter <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(2))
          secondAt <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(3))
          secondAfter <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(4))
        } yield {
          early shouldBe None
          firstAt shouldBe Some(TimeOfRequest(rc, tsWithSecs(1)))
          firstAfter shouldBe Some(TimeOfRequest(rc, tsWithSecs(1)))
          secondAt shouldBe Some(TimeOfRequest(rc + 2, tsWithSecs(3)))
          secondAfter shouldBe Some(TimeOfRequest(rc + 2, tsWithSecs(3)))
        }
      }

      "return the last request counter before a request timestamp should not care about the commit time" in {
        val store = mk()
        for {
          _ <- setupRequests(store)
          _ <- valueOrFail(
            store
              .replace(rc, tsWithSecs(1), Clean, Some(tsWithSecs(1000)))
          )(
            "replace1"
          )
          _ <- valueOrFail(
            store
              .replace(rc + 2, tsWithSecs(3), Clean, Some(tsWithSecs(3)))
          )(
            "replace2"
          )
          early <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(0))
          firstAt <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(1))
          firstAfter <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(2))
          secondAt <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(3))
          secondAfter <- store.lastRequestTimeWithRequestTimestampBeforeOrAt(tsWithSecs(4))
        } yield {
          early shouldBe None
          firstAt shouldBe Some(TimeOfRequest(rc, tsWithSecs(1)))
          firstAfter shouldBe Some(TimeOfRequest(rc, tsWithSecs(1)))
          secondAt shouldBe Some(TimeOfRequest(rc + 2, tsWithSecs(3)))
          secondAfter shouldBe Some(TimeOfRequest(rc + 2, tsWithSecs(3)))
        }
      }
    }
  }
}

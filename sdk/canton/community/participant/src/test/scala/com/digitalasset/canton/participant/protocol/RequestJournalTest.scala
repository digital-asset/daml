// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monad
import cats.data.OptionT
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.*
import com.digitalasset.canton.participant.protocol.RequestJournal.{
  RequestData,
  RequestState,
  RequestStateWithCursor,
}
import com.digitalasset.canton.participant.store.RequestJournalStore
import com.digitalasset.canton.participant.store.memory.InMemoryRequestJournalStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, RequestCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import java.time.Instant
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
class RequestJournalTest extends AsyncWordSpec with BaseTest with HasTestCloseContext {

  def mk(
      initRc: RequestCounter,
      store: RequestJournalStore = new InMemoryRequestJournalStore(loggerFactory),
  ): RequestJournal =
    new RequestJournal(store, mkSyncDomainMetrics, loggerFactory, initRc, FutureSupervisor.Noop)

  private def mkSyncDomainMetrics = ParticipantTestMetrics.domain

  def insertWithCursor(
      rj: RequestJournal,
      rc: RequestCounter,
      state: RequestStateWithCursor,
      requestTimestamp: CantonTimestamp,
      commitTime: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- rj.insert(rc, requestTimestamp)
      cursorF <- transitTo(rj, rc, requestTimestamp, state, commitTime).getOrElse(
        fail(s"Request $rc: Cursor future for request state $state")
      )
    } yield cursorF

  def transitTo(
      rj: RequestJournal,
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      target: RequestState,
      commitTimeO: Option[CantonTimestamp] = None,
  ): OptionT[Future, Unit] =
    OptionT[Future, Unit] {

      def step(
          unit: Option[Unit]
      ): Future[Either[Option[Unit], Option[Unit]]] =
        for {
          current <- rj
            .query(rc)
            .getOrElse(throw new IllegalArgumentException(s"request counter $rc not found"))
            .map(_.state)
          result <-
            if (current < target) {
              current.next.value match {
                case Clean =>
                  val commitTime =
                    commitTimeO.getOrElse(fail(s"No commit time for request $rc given"))
                  rj.terminate(rc, requestTimestamp, commitTime)
                    .map(u => Right(Some(u)))
                case Pending => fail("Next state must not be Pending.")
              }
            } else Future.successful(Right(unit))
        } yield result

      Monad[Future].tailRecM(Option.empty[Unit])(step)
    }

  def mkWith(
      initRc: RequestCounter,
      inserts: List[(RequestCounter, CantonTimestamp)],
      store: RequestJournalStore = new InMemoryRequestJournalStore(loggerFactory),
  ): Future[RequestJournal] = {
    val rj = mk(initRc, store)

    inserts
      .parTraverse_ { case (rc, timestamp) =>
        rj.insert(rc, timestamp)
      }
      .map(_ => rj)
  }

  def assertPresent(rj: RequestJournal, presentRcs: List[(RequestCounter, CantonTimestamp)])(
      expectedState: (RequestCounter, CantonTimestamp) => RequestData
  ): Future[Assertion] =
    presentRcs
      .parTraverse_ { case (rc, ts) =>
        rj.query(rc)
          .value
          .map(result =>
            assert(
              result.contains(expectedState(rc, ts)),
              s"Request counter $rc has state $result, but should be ${expectedState(rc, ts)}",
            )
          )
      }
      .map((_: Unit) => succeed)

  def assertAbsent(rj: RequestJournal, absentRcs: List[RequestCounter]): Future[Assertion] =
    absentRcs
      .parTraverse_ { rc =>
        rj.query(rc)
          .value
          .map(result => assert(result.isEmpty, s"Found state $result for request counter $rc"))
      }
      .map((_: Unit) => succeed)

  val commitTime: CantonTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2020-01-03T00:00:00.00Z"))

  "created with initial request counter 0" when {
    val initRc = RequestCounter(0)
    val rj = mk(initRc)

    "queries" should {
      s"return $None" in {
        assertAbsent(rj, List(initRc, initRc + 5, initRc - 1)).map { _ =>
          rj.numberOfDirtyRequests shouldBe 0
        }
      }
    }

    val inserts = Map(
      initRc -> CantonTimestamp.assertFromInstant(Instant.parse("2019-01-03T10:15:30.00Z")),
      initRc + 1 -> CantonTimestamp.assertFromInstant(Instant.parse("2019-01-03T10:15:31.00Z")),
      initRc + 4 -> CantonTimestamp.assertFromInstant(Instant.parse("2019-01-03T10:16:11.00Z")),
    )
    val nonInserts = List(initRc - 1, initRc + 2, initRc + 3, initRc + 5, RequestCounter.MaxValue)

    "after inserting request counters" should {

      "queries should return the right results" in {
        for {
          rj <- mkWith(initRc, inserts.toList)
          _ <- assertPresent(rj, inserts.toList)((rc, timestamp) =>
            RequestData(rc, Pending, timestamp)
          )
          _ <- assertAbsent(rj, nonInserts)
        } yield {
          rj.numberOfDirtyRequests shouldBe 3
        }
      }
    }

    s"transiting only the second request to $Clean" should {
      val transitRc = initRc + 1
      def setup(): Future[RequestJournal] =
        for {
          rj <- mkWith(initRc, inserts.toList)
          _ <- rj.terminate(transitRc, inserts(transitRc), inserts(transitRc).plusSeconds(1))
        } yield {
          rj
        }

      "query returns the updated result and leave the others unchanged" in {
        for {
          rj <- setup()
          _ <- assertPresent(rj, inserts.toList)((rc, timestamp) =>
            RequestData(
              rc,
              if (rc == transitRc) Clean else Pending,
              timestamp,
              Option.when(rc == transitRc)(timestamp.plusSeconds(1)),
              None,
            )
          )
          _ <- assertAbsent(rj, nonInserts)
        } yield rj.numberOfDirtyRequests shouldBe 2
      }
    }

    "inserting the requests again" should {
      "fail" in {
        for {
          rj <- mkWith(initRc, inserts.toList)
          _ <- MonadUtil.sequentialTraverse_(inserts) { case (rc, timestamp) =>
            loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
              rj.insert(rc, timestamp),
              _.getMessage should fullyMatch regex raw"The request .* is already pending\.",
            )
          }
          _ <- MonadUtil.sequentialTraverse_(inserts) { case (rc, timestamp) =>
            rj.terminate(rc, timestamp, timestamp)
          }
          _ <- MonadUtil.sequentialTraverse_(inserts) { case (rc, timestamp) =>
            loggerFactory.assertInternalErrorAsync[IllegalStateException](
              rj.insert(rc, timestamp),
              _.getMessage should fullyMatch regex raw"Map key .* already has value RequestData.* state = Clean.*\.",
            )
          }
        } yield succeed
      }
    }

    "modifying a nonexisting request" should {
      "fail" in {
        val rj = mk(initRc)
        for {
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            rj.terminate(initRc, CantonTimestamp.Epoch, commitTime),
            _.getMessage shouldBe "Cannot transit non-existing request with request counter 0",
          )
        } yield succeed
      }
    }

    "providing the wrong request timestamp" should {
      "fail" in {
        val rj = mk(initRc)
        for {
          _ <- rj.insert(initRc, CantonTimestamp.Epoch)
          _failure <- loggerFactory.assertInternalErrorAsync[IllegalStateException](
            rj.terminate(
              initRc,
              CantonTimestamp.ofEpochSecond(1),
              CantonTimestamp.ofEpochSecond(2),
            ),
            _.getMessage shouldBe s"Request 0: Inconsistent timestamps for request.\nStored: ${CantonTimestamp.Epoch}\nExpected: ${CantonTimestamp
                .ofEpochSecond(1)}",
          )
        } yield succeed
      }
    }

    "terminating with a too early commit time" should {
      "fail" in {
        for {
          rj <- mkWith(initRc, List(initRc -> CantonTimestamp.Epoch))
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            rj.terminate(
              initRc,
              CantonTimestamp.Epoch,
              CantonTimestamp.ofEpochMilli(-1),
            ),
            _.getMessage shouldBe "Request 0: Commit time 1969-12-31T23:59:59.999Z must be at least the request timestamp 1970-01-01T00:00:00Z",
          )
        } yield succeed
      }
    }
  }

  "created with a non-zero start value" when {
    val initRc = RequestCounter(0x100000000L)

    val insertsBelowCursor =
      List(
        (initRc - 1, CantonTimestamp.assertFromInstant(Instant.parse("2011-12-11T00:00:00.00Z"))),
        (RequestCounter(Long.MinValue), CantonTimestamp.ofEpochSecond(0)),
      )

    "inserting lower request counters" should {
      "fail" in {
        val rj = mk(initRc)
        MonadUtil
          .sequentialTraverse_(insertsBelowCursor) { case (rc, timestamp) =>
            loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
              rj.insert(rc, timestamp),
              _.getMessage should fullyMatch regex "The request counter .* is below the initial value .*",
            )
          }
          .map(_ => succeed)
      }
    }

    "inserting several request counters and progressing them somewhat" should {
      val initTs = CantonTimestamp.ofEpochSecond(1)
      val inserts = List(
        ((initRc + 4, CantonTimestamp.ofEpochSecond(5)), Pending, None),
        ((initRc + 2, CantonTimestamp.ofEpochSecond(3)), Clean, Some(commitTime)),
        ((initRc, initTs), Pending, None),
      )

      def setup(): Future[RequestJournal] =
        for {
          rj <- mkWith(initRc, inserts.map(_._1))
          _ <- inserts.parTraverse_ { case ((rc, timestamp), target, commitTime) =>
            transitTo(rj, rc, timestamp, target, commitTime).value
          }
        } yield rj

      "queries are correct" in {
        for {
          rj <- setup()
          _ <- inserts.parTraverse_ { case ((rc, timestamp), target, commitTime) =>
            rj.query(rc)
              .value
              .map(result =>
                assert(result.contains(new RequestData(rc, target, timestamp, commitTime, None)))
              )
          }
        } yield rj.numberOfDirtyRequests shouldBe 2
      }

      val insertedRc = initRc + 1
      val ts = CantonTimestamp.ofEpochSecond(2)

      "correctly count the requests" in {
        for {
          rj <- setup()
          _ <- rj.insert(insertedRc, ts)
          totalCount <- rj.size()
          countUntilTs <- rj.size(end = Some(ts))
          countFromTs <- rj.size(start = ts)
          count45 <- rj.size(
            CantonTimestamp.ofEpochSecond(4),
            Some(CantonTimestamp.ofEpochSecond(5)),
          )
          count56 <- rj.size(
            CantonTimestamp.ofEpochSecond(5),
            Some(CantonTimestamp.ofEpochSecond(6)),
          )
        } yield {
          totalCount shouldBe 4
          countUntilTs shouldBe 2
          countFromTs shouldBe 3
          count45 shouldBe 1
          count56 shouldBe 1
          rj.numberOfDirtyRequests shouldBe 3
        }
      }
    }
  }

  s"when created with head ${RequestCounter.MaxValue}" should {
    val initRc = RequestCounter.MaxValue
    val rj = mk(initRc)

    s"adding this request fails" in {
      for {
        _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
          rj.insert(initRc, CantonTimestamp.Epoch),
          _.getMessage shouldBe "The request counter 9223372036854775807 cannot be used.",
        )
      } yield succeed
    }
  }
}
@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
class RequestStateTest extends AnyWordSpec with BaseTest {

  "All request states" should {
    val testCases = Table[Option[RequestState], RequestState, Option[RequestState], Boolean](
      ("predecessor", "state", "successor", "has cursor"),
      (None, Pending, Some(Clean), false),
      (Some(Pending), Clean, None, true),
    )

    "correctly name their predecessors and successors" in {
      forEvery(testCases) { (pred, state, succ, _) =>
        assert(pred.forall(_ < state))
        assert(succ.forall(state < _))
        assert(state.compare(state) == 0)
        assert(state.next == succ)
      }
    }

    "correctly specify whether they have a cursor" in {
      forEvery(testCases) { (_, state, _, hasCursor) =>
        assert(state.hasCursor == hasCursor)
        assert(state.isInstanceOf[RequestStateWithCursor] == hasCursor)
      }
    }
  }
}

object RequestStateTest {
  val statesWithCursor: Array[RequestStateWithCursor] = Array(Clean)
}

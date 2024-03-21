// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableState.{
  LockCounter,
  PendingActivenessCheckCounter,
  PendingWriteCounter,
}
import com.digitalasset.canton.participant.store.{ConflictDetectionStore, HasPrunable}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DiscardOps, HasExecutorService, RequestCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class LockableStatesTest extends AsyncWordSpec with BaseTest with HasExecutorService {
  import ConflictDetectionHelpers.*
  import LockableStatesTest.*

  implicit val prettyString: Pretty[String] = PrettyUtil.prettyOfString(Predef.identity)

  private val toc0 = TimeOfChange(RequestCounter(0), CantonTimestamp.Epoch)
  private val toc1 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(1))
  private val toc2 = TimeOfChange(RequestCounter(2), CantonTimestamp.ofEpochSecond(2))
  private val toc3 = TimeOfChange(RequestCounter(3), CantonTimestamp.ofEpochSecond(3))
  private val tocEarly = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(-1))

  private val freshId = "FRESH"
  private val fresh2Id = "FRESH2"
  private val fresh3Id = "FRESH3"
  private val neitherFreeNorActiveId = "NEITHER_FREE_NOR_ACTIVE"
  private val freeId = "FREE"
  private val free2Id = "FREE2"
  private val activeId = "ACTIVE"
  private val evictableActiveId = "EVICTABLE_ACTIVE"
  private val evictableActive2Id = "EVICTABLE_ACTIVE2"
  private val freeNotEvictableId = "FREE_NOT_EVICTABLE"
  private lazy val preload = Map(
    neitherFreeNorActiveId -> StateChange(Status.neitherFreeNorActive, toc0),
    freeId -> StateChange(Status.free, toc0),
    free2Id -> StateChange(Status(-1), toc1),
    activeId -> StateChange(Status.active, toc0),
    evictableActiveId -> StateChange(Status.evictableActive, toc0),
    evictableActive2Id -> StateChange(Status.evictableActive, toc1),
    freeNotEvictableId -> StateChange(Status.freeNotEvictable, toc1),
  )

  private def mkSut(
      states: Map[StateId, StateChange[Status]] = Map.empty
  ): LockableStates[StateId, Status] =
    LockableStates.empty(
      new TestConflictDetectionStore(states, loggerFactory)(executorService),
      loggerFactory,
      timeouts,
      executionContext = executorService,
    )

  private def mkState(
      state: Option[StateChange[Status]] = None,
      activenessChecks: Int = 0,
      locks: Int = 0,
      writes: Int = 0,
  ): ImmutableLockableState[Status] =
    ImmutableLockableState(
      Some(state),
      PendingActivenessCheckCounter.assertFromInt(activenessChecks),
      LockCounter.assertFromInt(locks),
      PendingWriteCounter.assertFromInt(writes),
    )

  private def pendingAndCheck(
      sut: LockableStates[StateId, Status],
      rc: RequestCounter,
      check: ActivenessCheck[StateId],
  ): Future[(ActivenessCheckResult[StateId, Status], Seq[StateId])] = {
    val handle = sut.pendingActivenessCheck(rc, check)
    for {
      fetched <- sut.prefetchStates(handle.toBeFetched)
      _ = sut.providePrefetchedStates(handle, fetched)
      (locked, result) = sut.checkAndLock(handle)
    } yield (result, locked)
  }

  "prefetch" should {
    "report states to be fetched" in {
      val sut = mkSut()
      val check = mkActivenessCheck(
        fresh = Set(neitherFreeNorActiveId, freshId),
        free = Set(freeId, fresh3Id),
        active = Set(activeId, fresh2Id, free2Id),
        lock = Set(evictableActive2Id),
      )
      val handle = sut.pendingActivenessCheck(RequestCounter(0), check)
      handle.toBeFetched.toSet shouldBe (check.checkFresh ++ check.checkFree ++ check.checkActive ++ check.lock)
      handle.noOutstandingFetches shouldBe false
      handle.availableF.isCompleted shouldBe true // no further outstanding fetches other than the ones reported
    }

    "not report states that are already being fetched" in {
      val sut = mkSut()
      val check1 = mkActivenessCheck(
        fresh = Set(freshId),
        free = Set(freeId),
        active = Set(activeId),
        lock = Set(evictableActive2Id),
      )
      val handle1 = sut.pendingActivenessCheck(RequestCounter(1), check1)
      handle1.toBeFetched.toSet shouldBe Set(freshId, freeId, activeId, evictableActive2Id)

      val check2 = mkActivenessCheck(
        fresh = Set(freshId, fresh2Id),
        free = Set(freeId, free2Id),
        active = Set(activeId, evictableActiveId),
        lock = Set(evictableActive2Id, activeId, fresh3Id),
      )
      val handle2 = sut.pendingActivenessCheck(RequestCounter(2), check2)
      // Do not report items that are already being fetched
      handle2.toBeFetched.toSet shouldBe Set(fresh2Id, free2Id, evictableActiveId, fresh3Id)
      handle2.availableF.isCompleted shouldBe false // there are further outstanding fetches

      sut.providePrefetchedStates(handle2, Map.empty) // pretend none of them were found
      handle2.noOutstandingFetches shouldBe true
      handle2.availableF.isCompleted shouldBe false // there are still the outstanding fetches from handle1

      sut.providePrefetchedStates(handle1, Map.empty) // pretend none of them were found
      handle2.availableF.isCompleted shouldBe true
    }
  }

  "prefetchStates" should {
    "return the latest state from the store" in {
      val sut = mkSut(preload)
      val toBeFetched = Seq(freshId, freeId, activeId)
      for {
        fetched <- sut.prefetchStates(toBeFetched)
      } yield {
        fetched shouldBe preload.filter { case (id, _) => toBeFetched.contains(id) }
      }
    }
  }

  "check" should {
    "handle the empty check" in {
      val sut = mkSut()
      for {
        (result, locked) <- pendingAndCheck(sut, RequestCounter(0), ActivenessCheck.empty)
      } yield {
        result shouldBe mkActivenessCheckResult()
        locked shouldBe empty
      }
    }

    "check the conditions and lock the states" in {
      val sut = mkSut(preload)
      val all = Set(
        freshId,
        fresh2Id,
        fresh3Id,
        freeId,
        free2Id,
        activeId,
        neitherFreeNorActiveId,
        evictableActiveId,
      )
      val check = mkActivenessCheck(
        fresh = Set(neitherFreeNorActiveId, freshId),
        free = Set(freeId, evictableActiveId, fresh3Id),
        active = Set(activeId, fresh2Id, free2Id),
        lock = all,
      )

      for {
        (result, locked) <- pendingAndCheck(sut, RequestCounter(2), check)
      } yield {
        forEvery(all) { id =>
          sut.getInternalState(id) should contain(mkState(state = preload.get(id), locks = 1))
        }
        locked.toSet shouldBe all
        result shouldBe mkActivenessCheckResult(
          notFresh = Set(neitherFreeNorActiveId),
          unknown = Set(fresh2Id),
          notFree = Map(evictableActiveId -> preload(evictableActiveId).status),
          notActive = Map(free2Id -> preload(free2Id).status),
        )
      }
    }

    "report already locked states" in {
      val sut = mkSut(preload)
      val locked = Set(freshId, freeId, activeId)
      val check1 = mkActivenessCheck(lock = locked)
      val check2 = mkActivenessCheck(
        fresh = Set(freshId, fresh2Id),
        free = Set(freeId, free2Id),
        active = Set(activeId, evictableActiveId),
      )

      for {
        (result1, locked1) <- pendingAndCheck(sut, RequestCounter(2), check1)
        (result2, locked2) <- pendingAndCheck(sut, RequestCounter(3), check2)
      } yield {
        locked1.toSet shouldBe locked
        result1 shouldBe mkActivenessCheckResult(unknown = Set(freshId))
        locked2.toSet shouldBe Set.empty
        result2 shouldBe mkActivenessCheckResult(locked = locked)
      }
    }

    "lock items multiple times" in {
      val sut = mkSut(preload)
      val toBeLocked1 = Set(freshId, freeId, free2Id, activeId, evictableActiveId)
      val toBeLocked2 = Set(freshId, freeId, activeId)
      val check1 = mkActivenessCheck(lock = toBeLocked1)
      val check2 = mkActivenessCheck(
        lock = toBeLocked2,
        fresh = Set(freshId, fresh2Id),
        free = Set(freeId, free2Id),
        active = Set(activeId, evictableActiveId),
      )

      for {
        (result1, locked1) <- pendingAndCheck(sut, RequestCounter(2), check1)
        (result2, locked2) <- pendingAndCheck(sut, RequestCounter(3), check2)
      } yield {
        locked1.toSet shouldBe toBeLocked1
        result1 shouldBe mkActivenessCheckResult(unknown = Set(freshId))
        locked2.toSet shouldBe toBeLocked2
        result2 shouldBe mkActivenessCheckResult(locked = toBeLocked1)
        forEvery(toBeLocked1) { id =>
          val expectedLockCount = if (toBeLocked2.contains(id)) 2 else 1
          sut.getInternalState(id) should contain(
            mkState(state = preload.get(id), locks = expectedLockCount)
          )
        }
        toBeLocked2.foreach(sut.setStatusPendingWrite(_, Status.neitherFreeNorActive, toc3))
        forEvery(toBeLocked1) { id =>
          val expectedWriteCount = if (toBeLocked2.contains(id)) 1 else 0
          val expectedStatus =
            if (toBeLocked2.contains(id)) Some(StateChange(Status.neitherFreeNorActive, toc3))
            else preload.get(id)
          sut.getInternalState(id) should contain(
            mkState(state = expectedStatus, locks = 1, writes = expectedWriteCount)
          )
        }
      }
    }

    "keep evictable states in memory only if they are locked or have pending activations" in {
      val sut = mkSut(preload)
      val lock = Set(freshId, freeId, evictableActiveId)
      val check1 = mkActivenessCheck(fresh = Set(fresh3Id))
      val check2 = mkActivenessCheck(
        fresh = Set(freshId, fresh2Id, fresh3Id),
        free = Set(freeId, free2Id, freeNotEvictableId),
        active = Set(activeId, evictableActiveId, evictableActive2Id),
        lock = lock,
      )

      val handle1 = sut.pendingActivenessCheck(RequestCounter(1), check1)
      sut.providePrefetchedStates(handle1, Map.empty)
      for {
        (result, locked) <- pendingAndCheck(sut, RequestCounter(2), check2)
      } yield {
        locked.toSet shouldBe lock
        result shouldBe mkActivenessCheckResult()
        forEvery(lock) { id =>
          sut.getInternalState(id) should contain(mkState(preload.get(id), locks = 1))
        }
        forEvery(Seq(fresh2Id, free2Id, evictableActive2Id))(id =>
          sut.getInternalState(id) shouldBe None
        )
        forEvery(Seq(freeNotEvictableId, activeId)) { id =>
          sut.getInternalState(id) should contain(mkState(state = preload.get(id)))
        }
        sut.getInternalState(fresh3Id) should contain(mkState(activenessChecks = 1))
      }
    }

    "complain about outstanding prefetches" in {
      val sut = mkSut()
      val handle =
        sut.pendingActivenessCheck(RequestCounter(0), mkActivenessCheck(fresh = Set(freshId)))
      loggerFactory.assertThrowsAndLogs[IllegalConflictDetectionStateException](
        sut.checkAndLock(handle),
        _.errorMessage should include("An internal error has occurred."),
      )
    }

    "complain about outstanding prefetches by other requests" in {
      val sut = mkSut()
      val check = mkActivenessCheck(fresh = Set(freshId))
      sut.pendingActivenessCheck(RequestCounter(0), check).discard
      val handle2 = sut.pendingActivenessCheck(RequestCounter(1), check)
      loggerFactory.assertThrowsAndLogs[IllegalConflictDetectionStateException](
        sut.checkAndLock(handle2),
        _.errorMessage should include("An internal error has occurred."),
      )
    }

    "return the requested prior states unless they are locked" in {
      val sut = mkSut(preload)
      val check1 = mkActivenessCheck(
        fresh = Set(freshId),
        free = Set(freeId),
        active = Set(activeId, evictableActiveId),
        lock = Set(freshId, free2Id, activeId),
        prior = Set(freshId, freeId, activeId, evictableActiveId, free2Id),
      )
      val check2 = mkActivenessCheck(
        fresh = Set(freshId, fresh2Id),
        free = Set(freeId),
        active = Set(activeId, evictableActiveId),
        lock = Set(free2Id),
        prior = Set(freshId, fresh2Id, freeId, free2Id, activeId, evictableActiveId),
      )

      for {
        (result1, locked1) <- pendingAndCheck(sut, RequestCounter(2), check1)
        (result2, locked2) <- pendingAndCheck(sut, RequestCounter(3), check2)
      } yield {
        locked1.toSet shouldBe Set(freshId, free2Id, activeId)
        result1 shouldBe mkActivenessCheckResult(prior =
          Map(
            freshId -> None,
            freeId -> Some(Status.free),
            activeId -> Some(Status.active),
            evictableActiveId -> Some(Status.evictableActive),
            free2Id -> Some(Status(-1)),
          )
        )
        locked2.toSet shouldBe Set(free2Id)
        result2 shouldBe mkActivenessCheckResult(
          locked = Set(freshId, activeId, free2Id),
          prior = Map(
            fresh2Id -> None,
            freeId -> Some(Status.free),
            evictableActiveId -> Some(Status.evictableActive),
          ),
        )
      }

    }
  }

  "setStatusPendingWrite" should {
    "release the lock and set the pending write" in {
      val sut = mkSut(preload)
      val lock = Map(freshId -> Status(0), freeId -> Status(100), activeId -> Status(-100))
      val check = mkActivenessCheck(lock = lock.keySet)
      val rc = toc2.rc
      for {
        _ <- pendingAndCheck(sut, rc, check)
        _ = lock.foreach { case (id, newState) =>
          sut.setStatusPendingWrite(id, newState, toc2)
        }
      } yield {
        forEvery(lock) { case (id, newState) =>
          sut.getInternalState(id) should contain(
            mkState(state = Some(StateChange(newState, toc2)), writes = 1)
          )
        }
      }
    }

    "complain about missing locks" in {
      val sut = mkSut()
      a[IllegalConflictDetectionStateException] shouldBe thrownBy(
        sut.setStatusPendingWrite(freshId, Status(0), toc0)
      )
    }

    "keep the latest state in memory" in {
      val sut = mkSut(preload)
      val all = Set(freshId, freeId, activeId, evictableActiveId)
      val check = mkActivenessCheck(
        fresh = Set(freshId),
        free = Set(freeId),
        active = Set(activeId, evictableActiveId),
        lock = all,
      )
      for {
        _ <- pendingAndCheck(sut, tocEarly.rc, check)
        _ = all.foreach(id => sut.setStatusPendingWrite(id, Status(0), tocEarly))
      } yield {
        forEvery(all) { id =>
          val expectedState = preload.getOrElse(id, StateChange(Status(0), tocEarly))
          sut.getInternalState(id) should contain(mkState(state = Some(expectedState), writes = 1))
        }
      }
    }

    "deal with multiple pending writes" in {
      val sut = mkSut(preload)
      val check1 = mkActivenessCheck(lock = Set(freeId))
      for {
        _ <- pendingAndCheck(sut, toc1.rc, check1)
        _ <- pendingAndCheck(sut, toc2.rc, check1)
        _ = sut.setStatusPendingWrite(freeId, Status(0), toc1)
        _ = sut.getInternalState(freeId) should contain(
          mkState(state = Some(StateChange(Status(0), toc1)), locks = 1, writes = 1)
        )
        _ = sut.setStatusPendingWrite(freeId, Status(0), toc2)
      } yield {
        sut.getInternalState(freeId) should contain(
          mkState(state = Some(StateChange(Status(0), toc2)), writes = 2)
        )
      }
    }
  }

  "signalWriteAndTryEvict" should {
    "decrement the pending write counter and evict if safe" in {
      val sut = mkSut(preload)
      val lock = Map(
        freshId -> Status.evictableActive,
        freeId -> Status.freeNotEvictable,
        activeId -> Status.neitherFreeNorActive,
        evictableActiveId -> Status.free,
      )
      for {
        _ <- pendingAndCheck(sut, toc2.rc, mkActivenessCheck(lock = lock.keySet))
        _ = lock.foreach { case (id, newStatus) =>
          sut.setStatusPendingWrite(id, newStatus, toc2)
        }
        _ = lock.keys.foreach(id => sut.signalWriteAndTryEvict(toc2.rc, id))
      } yield {
        forEvery(lock) { case (id, newStatus) =>
          if (LockableStatus[Status].shouldEvict(newStatus)) {
            sut.getInternalState(id) shouldBe None
          } else {
            sut.getInternalState(id) should contain(
              mkState(state = Some(StateChange(newStatus, toc2)))
            )
          }
        }
      }
    }

    "fail if there is no pending write" in {
      val sut = mkSut()
      for {
        _ <- pendingAndCheck(sut, toc2.rc, mkActivenessCheck(lock = Set(freeId)))
      } yield {
        a[IllegalConflictDetectionStateException] should be thrownBy sut.signalWriteAndTryEvict(
          toc2.rc,
          freeId,
        )
        a[IllegalConflictDetectionStateException] should be thrownBy sut.signalWriteAndTryEvict(
          toc2.rc,
          activeId,
        )
        sut.setStatusPendingWrite(freeId, Status.freeNotEvictable, toc2)
        sut.signalWriteAndTryEvict(toc2.rc, freeId)
        a[IllegalConflictDetectionStateException] should be thrownBy sut.signalWriteAndTryEvict(
          toc2.rc,
          freeId,
        )
      }
    }

    "not evict if locks are held or there are other pending writes" in {
      val sut = mkSut(preload)
      val lock = Set(freeId, free2Id)
      for {
        _ <- pendingAndCheck(sut, toc2.rc, mkActivenessCheck(lock = lock))
        _ = sut.setStatusPendingWrite(freeId, Status.evictableActive, toc2)
        _ = sut.setStatusPendingWrite(free2Id, Status.evictableActive, toc2)
        _ <- pendingAndCheck(sut, toc3.rc, mkActivenessCheck(lock = Set(freeId, free2Id)))
        _ = sut.setStatusPendingWrite(freeId, Status.evictableActive, toc3)
        _ = lock.foreach(id => sut.signalWriteAndTryEvict(toc2.rc, id))
      } yield {
        sut.getInternalState(freeId) should contain(
          mkState(state = Some(StateChange(Status.evictableActive, toc3)), writes = 1)
        )
        sut.getInternalState(free2Id) should contain(
          mkState(state = Some(StateChange(Status.evictableActive, toc2)), locks = 1)
        )
      }
    }
  }

  "releaseLock" should {
    "release the lock once and try to evict" in {
      val sut = mkSut(preload)
      val lock1 =
        Set(
          freeId,
          freshId,
          fresh2Id,
          free2Id,
          evictableActiveId,
          evictableActive2Id,
          activeId,
          freeNotEvictableId,
        )
      val lock2 = Set(freeId, freshId, activeId, evictableActiveId)
      for {
        _ <- pendingAndCheck(sut, toc2.rc, mkActivenessCheck(lock = lock1))
        _ <- pendingAndCheck(sut, toc3.rc, mkActivenessCheck(lock = lock2))
      } yield {
        lock1.foreach(sut.releaseLock(toc2.rc, _))
        forEvery(lock2) { id =>
          sut.getInternalState(id) should contain(mkState(state = preload.get(id), locks = 1))
        }
        forEvery(Seq(fresh2Id, free2Id, evictableActive2Id))(id =>
          sut.getInternalState(id) shouldBe None
        )
        forEvery(Seq(freeNotEvictableId)) { id =>
          sut.getInternalState(id) should contain(mkState(state = preload.get(id)))
        }
      }
    }

  }
}

object LockableStatesTest {
  final case class Status(status: Int) extends PrettyPrinting with HasPrunable {
    override def pretty: Pretty[Status.this.type] = status => Pretty[Int].treeOf(status.status)
    override def prunable: Boolean = status < 0
  }
  object Status {
    implicit val lockableStatusStatus: LockableStatus[Status] = new LockableStatus[Status] {
      override def kind: String = "test"
      override def isFree(status: Status): Boolean = status.status <= 0
      override def isActive(status: Status): Boolean = status.status > 1
      override def shouldEvict(status: Status): Boolean = status.status < 3 && status.status > -10
    }

    val free: Status = Status(0)
    val freeNotEvictable: Status = Status(-10)
    val neitherFreeNorActive: Status = Status(1)
    val active: Status = Status(4)
    val evictableActive: Status = Status(2)
  }

  type StateId = String

  class TestConflictDetectionStore(
      states: Map[StateId, StateChange[Status]],
      override val loggerFactory: NamedLoggerFactory,
  )(implicit override val ec: ExecutionContext)
      extends ConflictDetectionStore[StateId, Status]
      with NamedLogging
      with InMemoryPrunableByTime {

    override def kind: String = "test"
    override def fetchStates(
        ids: Iterable[StateId]
    )(implicit traceContext: TraceContext): Future[Map[StateId, StateChange[Status]]] =
      Future.successful {
        val fetched = Map.newBuilder[StateId, StateChange[Status]]
        ids.foreach { id =>
          states.get(id).foreach(fetched += id -> _)
        }
        fetched.result()
      }

    override protected[canton] def doPrune(
        limit: CantonTimestamp,
        lastPruning: Option[CantonTimestamp],
    )(implicit
        traceContext: TraceContext
    ): Future[Int] = Future.successful(0)
  }
}

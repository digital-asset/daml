// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.data.{Chain, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.CantonTimestamp.{Epoch, ofEpochMilli}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetector.*
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableState.{
  LockCounter,
  PendingActivenessCheckCounter,
  PendingWriteCounter,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableStates.ConflictDetectionStoreAccessError
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.{
  AcsError,
  InvalidCommitSet,
  TransferStoreError,
}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  ChangeAfterArchival,
  ContractState as AcsContractState,
  DoubleContractArchival,
  DoubleContractCreation,
  Status,
  TransferredAway,
}
import com.digitalasset.canton.participant.store.TransferStore.{
  TransferAlreadyCompleted,
  TransferCompleted,
  UnknownTransferId,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.{
  InMemoryTransferStore,
  TransferCache,
  TransferCacheTest,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId, TransferId}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutorService, RequestCounter, TransferCounter}
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
class ConflictDetectorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with ConflictDetectionHelpers {

  import ConflictDetectionHelpers.*
  import TransferStoreTest.*

  private val coid00: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val coid01: LfContractId = ExampleTransactionFactory.suffixedId(0, 1)
  private val coid10: LfContractId = ExampleTransactionFactory.suffixedId(1, 0)
  private val coid11: LfContractId = ExampleTransactionFactory.suffixedId(1, 1)
  private val coid20: LfContractId = ExampleTransactionFactory.suffixedId(2, 0)
  private val coid21: LfContractId = ExampleTransactionFactory.suffixedId(2, 1)
  private val coid22: LfContractId = ExampleTransactionFactory.suffixedId(2, 2)

  private val transfer1 = TransferId(sourceDomain1, Epoch)
  private val transfer2 = TransferId(sourceDomain2, Epoch)

  private val initialTransferCounter: TransferCounter = TransferCounter.Genesis
  private val transferCounter1 = initialTransferCounter + 1
  private val transferCounter2 = initialTransferCounter + 2

  private val active = Active(initialTransferCounter)

  private def defaultTransferCache: TransferCache =
    new TransferCache(new InMemoryTransferStore(targetDomain, loggerFactory), loggerFactory)

  private def mkCd(
      acs: ActiveContractStore = mkEmptyAcs(),
      transferCache: TransferCache = defaultTransferCache,
  ): ConflictDetector =
    new ConflictDetector(
      acs,
      transferCache,
      loggerFactory,
      true,
      parallelExecutionContext,
      timeouts,
      futureSupervisor,
      testedProtocolVersion,
    )

  "ConflictDetector" should {

    "handle the empty request with the empty store" in {
      val cd = mkCd()
      for {
        _ <- singleCRwithTR(
          cd,
          RequestCounter(0),
          ActivenessSet.empty,
          mkActivenessResult(),
          CommitSet.empty,
          Epoch,
        )
      } yield succeed
    }

    "handle a single request" in {
      val rc = RequestCounter(10)
      val ts = ofEpochMilli(1)
      val toc = TimeOfChange(rc, ts)
      val tocN5 = TimeOfChange(rc - 5L, Epoch)

      for {
        acs <- mkAcs(
          (coid00, tocN5, active),
          (coid01, tocN5, active),
        )
        cd = mkCd(acs)

        actSet = mkActivenessSet(
          deact = Set(coid00),
          useOnly = Set(coid01),
          create = Set(coid10),
          prior = Set(coid00, coid10),
        )
        actRes = mkActivenessResult(prior = Map(coid00 -> Some(active), coid10 -> None))
        commitSet = mkCommitSet(
          arch = Set(coid00),
          create = Set(coid10),
        )
        _ <- singleCRwithTR(cd, rc, actSet, actRes, commitSet, ts)

        _ <- checkContractState(acs, coid00, Archived -> toc)("consumed contract is archived")
        _ <- checkContractState(acs, coid01, active -> tocN5)("fetched contract remains active")
        _ <- checkContractState(acs, coid10, active -> toc)("created contract is active")
      } yield succeed
    }

    "report nonexistent contracts" in {
      val rc = RequestCounter(10)
      val ts = CantonTimestamp.assertFromInstant(Instant.parse("2029-01-01T00:00:00.00Z"))
      val toc = TimeOfChange(rc, ts)
      val tocN5 = TimeOfChange(rc - 5L, Epoch)
      val tocN3 = TimeOfChange(rc - 3L, ofEpochMilli(1))
      for {
        acs <- mkAcs(
          (coid00, tocN5, active),
          (coid01, tocN5, active),
          (coid00, tocN3, Archived),
        )
        cd = mkCd(acs = acs)

        actSet = mkActivenessSet(
          deact = Set(coid00, coid01, coid10),
          useOnly = Set(coid21),
          create = Set(coid11),
        )
        actRes = mkActivenessResult(
          unknown = Set(coid10, coid21),
          notActive = Map(coid00 -> Archived),
        )
        _ <- singleCRwithTR(
          cd,
          rc,
          actSet,
          actRes,
          mkCommitSet(arch = Set(coid01), create = Set(coid11)),
          ts,
        )
        _ <- checkContractState(acs, coid00, (Archived, tocN3))(
          "archived contract remains archived"
        )
        _ <- checkContractState(acs, coid01, (Archived, toc))(
          "active contract gets archived at commit time"
        )
        _ <- checkContractState(acs, coid11, (active, toc))(
          "contract 11 gets created at request time"
        )
      } yield succeed
    }

    "complain about failing ACS reads" in {
      val cd = mkCd(acs = new ThrowingAcs[RuntimeException](msg => new RuntimeException(msg)))
      for {
        failure <- cd
          .registerActivenessSet(RequestCounter(0), mkActivenessSet(deact = Set(coid00)))
          .failed
      } yield assert(failure.isInstanceOf[ConflictDetectionStoreAccessError])
    }

    "complain about requests in flight" in {
      val cd = mkCd()
      for {
        _ <- cd.registerActivenessSet(RequestCounter(0), ActivenessSet.empty).failOnShutdown
        _ <- loggerFactory.assertInternalErrorAsync[IllegalConflictDetectionStateException](
          cd.registerActivenessSet(RequestCounter(0), ActivenessSet.empty).failOnShutdown,
          _.getMessage shouldBe "Request 0 is already in flight.",
        )
        cr <- cd.checkActivenessAndLock(RequestCounter(0)).failOnShutdown
        _ <- loggerFactory.assertInternalErrorAsync[IllegalConflictDetectionStateException](
          cd.registerActivenessSet(RequestCounter(0), ActivenessSet.empty).failOnShutdown,
          _.getMessage shouldBe "Request 0 is already in flight.",
        )
        _ <- loggerFactory.assertInternalErrorAsync[IllegalConflictDetectionStateException](
          cd.checkActivenessAndLock(RequestCounter(0)).failOnShutdown,
          _.getMessage shouldBe "Request 0 has no pending activeness check.",
        )
        fin <- cd
          .finalizeRequest(CommitSet.empty, TimeOfChange(RequestCounter(0), Epoch))
          .flatten
          .failOnShutdown
      } yield {
        cr shouldBe mkActivenessResult()
        fin shouldBe Right(())
      }
    }

    "complain about request without prefetching" in {
      val cd = mkCd()
      for {
        _ <- loggerFactory.assertInternalErrorAsync[IllegalConflictDetectionStateException](
          cd.checkActivenessAndLock(RequestCounter(0)).failOnShutdown,
          _.getMessage shouldBe "Request 0 has no pending activeness check.",
        )
      } yield succeed
    }

    "complain about nonexistent requests at finalization" in {
      val cd = mkCd()
      for {
        error <- cd.finalizeRequest(CommitSet.empty, TimeOfChange(RequestCounter(0), Epoch)).failed
      } yield assert(error.isInstanceOf[IllegalArgumentException])
    }

    "complain about requests in flight while the changes are written" in {
      val rc = RequestCounter(0)

      for {
        rawAcs <- mkAcs()
        acs = new HookedAcs(rawAcs)
        cd = mkCd(acs)

        cr <- prefetchAndCheck(cd, rc, mkActivenessSet(create = Set(coid00)))
        _ = acs.setCreateHook { (_, _) =>
          // Insert the same request with a different activeness set while the ACS updates happen
          loggerFactory
            .assertInternalErrorAsync[IllegalConflictDetectionStateException](
              cd.registerActivenessSet(rc, ActivenessSet.empty).failOnShutdown,
              _.getMessage shouldBe "Request 0 is already in flight.",
            )
            .void
        }
        fin <- cd
          .finalizeRequest(mkCommitSet(create = Set(coid00)), TimeOfChange(rc, Epoch))
          .flatten
          .failOnShutdown
      } yield {
        cr shouldBe mkActivenessResult()
        fin shouldBe Right(())
      }
    }

    "lock created and archived contracts and updated keys" in {
      val rc = RequestCounter(10)
      val toc1 = TimeOfChange(rc - 5L, Epoch)
      val toc2 = TimeOfChange(rc - 1L, ofEpochMilli(1))
      for {
        rawAcs <- mkAcs(
          (coid00, toc1, active),
          (coid01, toc1, active),
          (coid10, toc2, active),
          (coid11, toc1, active),
          (coid10, toc2, Archived),
        )
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        actSet = mkActivenessSet(
          deact = Set(coid00, coid01, coid10),
          useOnly = Set(coid11, coid20),
          create = Set(coid21, coid22),
        )
        cr <- prefetchAndCheck(cd, rc, actSet)

        _ = assert(
          cr == mkActivenessResult(unknown = Set(coid20), notActive = Map(coid10 -> Archived))
        )
        _ = checkContractState(cd, coid00, active, toc1, 0, 1, 0)(s"lock contract $coid00")
        _ = checkContractState(cd, coid01, active, toc1, 0, 1, 0)(s"lock contract $coid01")
        _ = checkContractState(cd, coid10, Archived, toc2, 0, 1, 0)(
          s"lock archived contract $coid10"
        )
        _ = checkContractStateAbsent(cd, coid11)(s"evict used only contract $coid11")
        _ = checkContractStateAbsent(cd, coid20)(s"do not keep non-existent contract $coid20")
        _ = checkContractState(cd, coid21, 0, 1, 0)(s"lock contract $coid21 for creation")
        _ = checkContractState(cd, coid22, 0, 1, 0)(s"lock contract $coid22 for creation")

        toc = TimeOfChange(rc, ofEpochMilli(2))
        _ = acs.setCreateHook { (coids, ToC) =>
          Future.successful {
            assert(coids.toSet == Set(coid21 -> initialTransferCounter) && ToC == toc)
            checkContractState(cd, coid21, active, toc, 0, 0, 1)(s"Contract $coid01 is active")
            checkContractStateAbsent(cd, coid22)(
              s"Rolled-back creation for contract $coid22 is evicted"
            )
          }
        }
        _ = acs.setArchiveHook { (coids, ToC) =>
          Future.successful {
            assert(coids.toSet == Set(coid00) && ToC == toc)
            checkContractState(cd, coid00, Archived, toc, 0, 0, 1)(
              s"Contract $coid00 is archived with pending write"
            )
            checkContractStateAbsent(cd, coid01)(s"Non-archived contract $coid01 is evicted.")
          }
        }
        fin <- cd
          .finalizeRequest(
            mkCommitSet(
              arch = Set(coid00),
              create = Set(coid21),
            ),
            toc,
          )
          .flatten
          .failOnShutdown
        _ = assert(fin == Right(()))

        _ = checkContractStateAbsent(cd, coid00)(s"evict archived contract $coid00")
        _ = checkContractStateAbsent(cd, coid01)(s"evict unlocked non-archived contract $coid01")
        _ = checkContractStateAbsent(cd, coid21)(s"evict created contract $coid21")
      } yield succeed
    }

    "rollback archival while contract is being created" in {
      val rc = RequestCounter(10)
      val ts = ofEpochMilli(10)
      val toc = TimeOfChange(rc, ts)
      val cd = mkCd()
      val actSet0 =
        mkActivenessSet(create = Set(coid00, coid01))
      val commitSet0 =
        mkCommitSet(create = Set(coid00, coid01))
      val actSet1 =
        mkActivenessSet(deact = Set(coid00))
      for {
        cr0 <- prefetchAndCheck(cd, rc, actSet0)
        _ = cr0 shouldBe mkActivenessResult()
        cr1 <- prefetchAndCheck(cd, rc + 1L, actSet1)
        _ = cr1 shouldBe mkActivenessResult(locked = Set(coid00))
        _ = checkContractState(cd, coid00, 0, 1 + 1, 0)(
          s"Nonexistent contract $coid00 is locked for activation and deactivation"
        )

        fin1 <- cd
          .finalizeRequest(CommitSet.empty, TimeOfChange(rc + 1, ts.plusMillis(1)))
          .flatten
          .failOnShutdown
        _ = assert(fin1 == Right(()))
        _ = checkContractState(cd, coid00, 0, 1, 0)(
          s"Rollback of request ${rc + 1} releases the deactivation lock."
        )

        fin0 <- cd.finalizeRequest(commitSet0, toc).flatten.failOnShutdown
        _ = fin0 shouldBe Right(())
        _ = forEvery(Seq(coid00, coid01)) { coid =>
          checkContractStateAbsent(cd, coid)(s"created contract $coid is evicted")
        }
      } yield succeed
    }

    "detect conflicts" in {
      val rc = RequestCounter(10)
      val ts = CantonTimestamp.assertFromInstant(Instant.parse("2050-10-11T00:00:10.00Z"))
      val toc = TimeOfChange(rc, ts)
      val toc0 = TimeOfChange(RequestCounter(0), ts.minusMillis(10))
      val toc1 = TimeOfChange(RequestCounter(1), ts.minusMillis(5))
      val toc2 = TimeOfChange(RequestCounter(2), ts.minusMillis(1))
      for {
        rawAcs <- mkAcs(
          (coid00, toc0, active),
          (coid01, toc0, active),
          (coid01, toc1, Archived),
          (coid10, toc2, active),
          (coid11, toc2, active),
        )
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        // Activeness check for the first request
        actSet1 = mkActivenessSet(
          deact = Set(coid00, coid11),
          useOnly = Set(coid10),
          create = Set(coid20, coid21),
        )
        cr1 <- prefetchAndCheck(cd, rc, actSet1)
        _ = cr1 shouldBe mkActivenessResult()
        _ = checkContractState(cd, coid00, active, toc0, 0, 1, 0)(
          s"locked consumed contract $coid00"
        )
        _ = checkContractState(cd, coid11, active, toc2, 0, 1, 0)(
          s"locked consumed contract $coid01"
        )
        _ = checkContractStateAbsent(cd, coid10)(s"evict used-only contract $coid10")

        // Prefetch a third request
        actSet3 = mkActivenessSet(
          deact = Set(coid00, coid11),
          useOnly = Set(coid01, coid10, coid21),
          prior = Set(coid01, coid21),
        )
        actRes3 = mkActivenessResult(
          locked = Set(coid00, coid10, coid21),
          notActive = Map(coid01 -> Archived, coid11 -> Archived),
          prior = Map(coid01 -> Some(Archived)),
        )
        _ <- cd.registerActivenessSet(rc + 2, actSet3).failOnShutdown

        // Activeness check for the second request
        rc2 = rc + 1L
        actSet2 = mkActivenessSet(
          deact = Set(coid00, coid10, coid20, coid21),
          useOnly = Set(coid11, coid01),
          prior = Set(coid00, coid10, coid20),
        )
        actRes2 = mkActivenessResult(
          locked = Set(coid00, coid11, coid20, coid21),
          notActive = Map(coid01 -> Archived),
          prior = Map(coid10 -> Some(active)),
        )
        cr2 <- prefetchAndCheck(cd, rc2, actSet2)
        _ = assert(cr2 == actRes2)
        _ = checkContractState(cd, coid00, active, toc0, 1, 2, 0)(s"locked $coid00 twice")
        _ = checkContractState(cd, coid10, active, toc2, 1, 1, 0)(
          s"locked $coid10 once by request $rc2"
        )
        _ = checkContractState(cd, coid11, active, toc2, 1, 1, 0)(
          s"used-only contract $coid11 remains locked once"
        )
        _ = checkContractState(cd, coid01, Archived, toc1, 1, 0, 0)(
          s"keep inactive contract $coid01 with pending activeness check"
        )
        _ = checkContractState(cd, coid20, 0, 1 + 1, 0)(s"Contract $coid20 in creation is locked")
        _ = checkContractState(cd, coid21, 1, 1 + 1, 0)(s"Contract $coid21 in creation is locked")

        // Check that the in-memory states of contracts are as expected after finalizing the first request, but before the updates are persisted
        _ = acs.setCreateHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid20, active, toc, 0, 1, 1)(s"Contract $coid20 remains locked")
            checkContractState(cd, coid21, 1, 1, 0)(
              s"Contract $coid21 is rolled back and remains locked"
            )
          }
        }
        _ = acs.setArchiveHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid00, active, toc0, 1, 1, 0)(s"$coid00 remains locked once")
            checkContractState(cd, coid11, Archived, toc, 1, 0, 1)(s"$coid11 is being archived")
          }
        }
        commitSet1 = mkCommitSet(
          arch = Set(coid11),
          create = Set(coid20),
        )
        fin1 <- cd.finalizeRequest(commitSet1, toc).flatten.failOnShutdown
        _ = assert(fin1 == Right(()))
        _ = checkContractState(cd, coid00, active, toc0, 1, 1, 0)(s"$coid00 remains locked once")
        _ = checkContractState(cd, coid11, Archived, toc, 1, 0, 0)(
          s"Archived $coid11 remains due to pending activation check"
        )
        _ = checkContractState(cd, coid20, active, toc, 0, 1, 0)(
          s"Created contract $coid20 remains locked"
        )
        _ = checkContractState(cd, coid21, 1, 1, 0)(s"Rolled back $coid21 remains locked")

        // Activeness check for the third request
        cr3 <- cd.checkActivenessAndLock(rc + 2).failOnShutdown
        _ = assert(cr3 == actRes3)
        _ = checkContractState(cd, coid00, active, toc0, 0, 2, 0)(
          s"Contract $coid00 is locked twice"
        )
        _ = checkContractStateAbsent(cd, coid01)(s"Inactive contract $coid01 is not kept in memory")
        _ = checkContractState(cd, coid11, Archived, toc, 0, 1, 0)(
          s"Archived contract $coid11 is locked nevertheless"
        )
        _ = checkContractState(cd, coid10, active, toc2, 0, 1, 0)(
          s"Used-only contract $coid10 remains locked once"
        )
      } yield succeed
    }

    "detect duplicate creates" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(1))
      for {
        rawAcs <- mkAcs((coid00, toc0, active))
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        // Activeness check for the first request
        actSet0 = mkActivenessSet(
          create = Set(coid00, coid01, coid11)
        )
        cr0 <- prefetchAndCheck(cd, RequestCounter(1), actSet0)
        _ = assert(
          cr0 == mkActivenessResult(notFresh = Set(coid00))
        )
        _ = checkContractState(cd, coid00, active, toc0, 0, 1, 0)(
          s"lock for activation the existing contract $coid00"
        )
        _ = checkContractState(cd, coid01, 0, 1, 0)(
          s"lock non-existing contract $coid01 for creation"
        )

        // Activeness check for the second request
        actSet1 = mkActivenessSet(
          deact = Set(coid11),
          create = Set(coid01, coid10),
        )
        actRes1 = mkActivenessResult(locked = Set(coid11, coid01))
        cr1 <- prefetchAndCheck(cd, RequestCounter(2), actSet1)
        _ = assert(cr1 == actRes1)
        _ = checkContractState(cd, coid01, 0, 2, 0)(
          s"Contract $coid01 is locked twice for activation"
        )
        _ = checkContractState(cd, coid10, 0, 1, 0)(s"lock contract $coid10 for creation")
        _ = checkContractState(cd, coid11, 0, 1 + 1, 0)(
          s"locked-for-creation contract $coid11 is locked for deactivation"
        )

        // Finalize first request and make sure that the in-memory states are up to date while the ACS updates are being written
        _ = acs.setCreateHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid01, active, toc1, 0, 1, 1)(
              s"Contract $coid01 is being created"
            )
            checkContractState(cd, coid11, 0, 1, 0)(s"Rolled-back contract $coid11 remains locked")
          }
        }
        fin1 <- cd
          .finalizeRequest(mkCommitSet(create = Set(coid01)), toc1)
          .flatten
          .failOnShutdown
        _ = assert(fin1 == Right(()))
      } yield succeed
    }

    "support transient contracts" in {
      val rc = RequestCounter(0)
      val ts = ofEpochMilli(100)
      val toc = TimeOfChange(rc, ts)
      for {
        acs <- mkAcs()
        cd = mkCd(acs)
        actSet = mkActivenessSet(
          create = Set(coid00, coid01, coid10, coid11),
          prior = Set(coid00),
        )
        actRes = mkActivenessResult(prior = Map(coid00 -> None))
        commitSet = mkCommitSet(
          arch = Set(coid00, coid10),
          create = Set(coid01, coid00),
        )
        _ <- singleCRwithTR(cd, rc, actSet, actRes, commitSet, ts)

        _ <- checkContractState(acs, coid00, (Archived, toc))(
          s"transient contract $coid00 is archived"
        )
        _ <- checkContractState(acs, coid01, (active, toc))(s"contract $coid01 is created")
        _ <- checkContractState(acs, coid10, (Archived, toc))(
          s"contract $coid10 is archived, but not created"
        )
        _ <- checkContractState(acs, coid11, None)(s"contract $coid11 does not exist")
      } yield succeed
    }

    "handle double archival" in {
      val rc = RequestCounter(10)
      val ts = ofEpochMilli(100)
      val ts1 = ts.plusMillis(1)
      val ts2 = ts.plusMillis(3)

      val toc = TimeOfChange(rc, ts)
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      val toc1 = TimeOfChange(rc + 1, ts1)
      val toc2 = TimeOfChange(rc + 2, ts2)

      for {
        rawAcs <- mkAcs(
          (coid00, toc0, active),
          (coid01, toc0, active),
          (coid10, toc0, active),
          (coid11, toc0, active),
          (coid20, toc0, active),
        )
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        // Prefetch three requests in reversed order
        _ <- cd.registerActivenessSet(rc + 2L, mkActivenessSet(deact = Set(coid10))).failOnShutdown
        _ <- cd
          .registerActivenessSet(
            rc + 1,
            mkActivenessSet(deact = Set(coid00, coid10, coid11, coid20)),
          )
          .failOnShutdown
        _ <- cd
          .registerActivenessSet(
            rc,
            mkActivenessSet(deact = Set(coid00, coid01, coid10, coid20)),
          )
          .failOnShutdown

        // Activeness check for first request
        cr0 <- cd.checkActivenessAndLock(rc).failOnShutdown
        _ = cr0 shouldBe mkActivenessResult()

        // Activeness check for second request
        cr1 <- cd.checkActivenessAndLock(rc + 1).failOnShutdown
        _ = assert(cr1 == mkActivenessResult(locked = Set(coid00, coid10, coid20)))
        _ = Seq((coid00, 2), (coid10, 2), (coid01, 1), (coid11, 1), (coid20, 2)).foreach {
          case (coid, locks) =>
            checkContractState(cd, coid, active, toc0, if (coid == coid10) 1 else 0, locks, 0)(
              s"Contract $coid is locked by $locks requests"
            )
        }

        // Finalize second request
        _ = acs.setArchiveHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid00, Archived, toc1, 0, 1, 1)(
              s"Archival for $coid00 retains the lock for the other request"
            )
            checkContractState(cd, coid11, Archived, toc1, 0, 0, 1)(
              s"Contract $coid11 is being archived"
            )
            checkContractState(cd, coid10, active, toc0, 1, 1, 0)(
              s"Lock on $coid10 is released once"
            )
          }
        }
        fin1 <- cd
          .finalizeRequest(mkCommitSet(arch = Set(coid00, coid11, coid20)), toc1)
          .flatten
          .failOnShutdown
        _ = assert(fin1 == Right(()))
        _ <- List(coid00 -> 1, coid11 -> 0, coid20 -> 1).parTraverse_ { case (coid, locks) =>
          if (locks > 0) {
            checkContractState(cd, coid, Archived, toc1, 0, locks, 0)(
              s"Archived contract $coid is retained because of more locks"
            )
          } else {
            checkContractStateAbsent(cd, coid)(s"Archived contract $coid is evicted")
          }
          checkContractState(acs, coid, (Archived, toc1))(
            s"contract $coid is archived by request ${rc + 1L}"
          )
        }

        // Activeness check for third request
        cr2 <- cd.checkActivenessAndLock(rc + 2L).failOnShutdown
        _ = assert(cr2 == mkActivenessResult(locked = Set(coid10)))

        // Finalize first request
        _ = acs.setArchiveHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid00, Archived, toc1, 0, 0, 1)(
              s"Double archived contract $coid00 has a pending write"
            )
            checkContractState(cd, coid01, Archived, toc, 0, 0, 1)(
              s"Contract $coid01 is being archived"
            )
            checkContractState(cd, coid10, Archived, toc, 0, 1, 1)(
              s"Contract $coid10 is being archived"
            )
            checkContractStateAbsent(cd, coid20)(
              s"Unlocking the archived contract $coid20 leaves it evicted"
            )
          }
        }
        fin0 <- cd
          .finalizeRequest(mkCommitSet(arch = Set(coid00, coid01, coid10)), toc)
          .flatten
          .failOnShutdown
        _ = assert(
          fin0 == Left(NonEmptyChain(AcsError(DoubleContractArchival(coid00, toc1, toc)))),
          s"double archival of $coid00 is reported",
        )
        _ <- checkContractState(acs, coid00, (Archived, toc1))(
          s"contract $coid00 is double archived by request $rc"
        )
        _ <- List(coid01, coid10).parTraverse_ { coid =>
          checkContractState(acs, coid, (Archived, toc))(s"contract $coid is archived as usual")
        }

        // Finalize third request
        fin2 <- cd.finalizeRequest(mkCommitSet(arch = Set(coid10)), toc2).flatten.failOnShutdown
        _ = assert(
          fin2 == Left(NonEmptyChain(AcsError(DoubleContractArchival(coid10, toc, toc2)))),
          s"double archival of $coid01 is reported",
        )
        _ <- checkContractState(acs, coid10, (Archived, toc2))(
          s"contract archival for $coid10 is overwritten"
        )
      } yield succeed
    }

    "lock inactive contracts for deactivation" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(1))
      for {
        acs <- mkAcs(
          (coid00, toc0, active),
          (coid01, toc0, active),
          (coid01, toc0, Archived),
        )
        cd = mkCd(acs)

        cr0 <- prefetchAndCheck(cd, toc1.rc, mkActivenessSet(deact = Set(coid00, coid01)))
        _ = assert(cr0 == mkActivenessResult(notActive = Map(coid01 -> Archived)))
        _ = checkContractState(cd, coid01, Archived, toc0, 0, 1, 0)(
          s"Archived contract $coid01 is locked."
        )
        fin0 <- cd
          .finalizeRequest(mkCommitSet(arch = Set(coid00, coid01)), toc1)
          .flatten
          .failOnShutdown
        _ = assert(fin0 == Left(Chain.one(AcsError(DoubleContractArchival(coid01, toc0, toc1)))))
        _ = checkContractStateAbsent(cd, coid01)(s"Double archived contract remains archived")
        _ <- checkContractState(acs, coid00, (Archived, toc1))(s"contract $coid00 gets archived")
      } yield succeed
    }

    "lock existing contracts for activation" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(2))
      for {
        acs <- mkAcs((coid00, toc0, active), (coid01, toc0, active), (coid01, toc0, Archived))
        cd = mkCd(acs)

        cr1 <- prefetchAndCheck(cd, toc1.rc, mkActivenessSet(create = Set(coid01, coid10)))
        _ = assert(cr1 == mkActivenessResult(notFresh = Set(coid01)))
        _ = checkContractState(cd, coid01, Archived, toc0, 0, 1, 0)(
          s"Existing contract $coid01 is locked."
        )
        fin1 <- cd
          .finalizeRequest(mkCommitSet(create = Set(coid01, coid10)), toc1)
          .flatten
          .failOnShutdown
        _ = assert(
          fin1.leftMap(_.toList.toSet) == Left(
            Set(
              AcsError(DoubleContractCreation(coid01, toc0, toc1)),
              AcsError(ChangeAfterArchival(coid01, toc0, toc1)),
            )
          )
        )
        _ <- checkContractState(acs, coid10, (active, toc1))(s"contract $coid10 is created")
      } yield succeed
    }

    "complain about invalid commit set" in {
      def checkInvalidCommitSet(cd: ConflictDetector, rc: RequestCounter, ts: CantonTimestamp)(
          activenessSet: ActivenessSet,
          commitSet: CommitSet,
      )(clue: String): Future[Assertion] =
        for {
          cr <- prefetchAndCheck(cd, rc, activenessSet)
          _ = assert(cr == mkActivenessResult(), clue)
          error <- loggerFactory.suppressWarningsAndErrors {
            cd.finalizeRequest(commitSet, TimeOfChange(rc, ts)).flatten.failOnShutdown.transform {
              case Failure(t) => Success(t)
              case Success(v) => Failure(new NoSuchElementException(s"Future did not fail. $clue"))
            }
          }
        } yield assert(error.isInstanceOf[InvalidCommitSet])

      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs((coid00, toc0, active))
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(acs, transferCache)

        _ <- checkInvalidCommitSet(cd, RequestCounter(1), ofEpochMilli(2))(
          mkActivenessSet(deact = Set(coid00)),
          mkCommitSet(arch = Set(coid00, coid10)),
        )("Archive non-locked contract")
        _ <- checkContractState(acs, coid00, (active, toc0))(s"contract $coid00 remains active")

        _ <- checkInvalidCommitSet(cd, RequestCounter(2), ofEpochMilli(2))(
          mkActivenessSet(create = Set(coid01)),
          mkCommitSet(create = Set(coid01, coid10)),
        )("Create non-locked contract")
        _ <- checkContractState(acs, coid01, None)(s"contract $coid01 remains non-existent")

        _ <- checkInvalidCommitSet(cd, RequestCounter(3), ofEpochMilli(3))(
          mkActivenessSet(tfIn = Set(coid01), transferIds = Set(transfer1, transfer2)),
          mkCommitSet(tfIn = Map(coid00 -> transfer1, coid01 -> transfer2)),
        )("Transferred-in contract not locked.")

        _ <- checkInvalidCommitSet(cd, RequestCounter(4), ofEpochMilli(4))(
          mkActivenessSet(useOnly = Set(coid00)),
          mkCommitSet(tfOut = Map(coid00 -> (sourceDomain1.unwrap -> transferCounter1))),
        )("Transfer-out contract only used, not locked.")
      } yield succeed
    }

    "opportunistic follow-up" in {
      val rc = RequestCounter(10)
      val ts = ofEpochMilli(10)
      val toc0 = TimeOfChange(rc, ts)
      val toc1 = TimeOfChange(rc + 1L, ts.plusMillis(1))

      for {
        rawAcs <- mkAcs()
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        // Activeness check of first request
        actSet0 = mkActivenessSet(create = Set(coid00, coid01, coid10))
        cr0 <- prefetchAndCheck(cd, rc, actSet0)
        _ = cr0 shouldBe mkActivenessResult()

        // Activeness check of second request
        cr1 <- prefetchAndCheck(
          cd,
          rc + 1,
          mkActivenessSet(deact = Set(coid00, coid10), prior = Set(coid00, coid10)),
        )
        _ = cr1 shouldBe mkActivenessResult(locked = Set(coid00, coid10))
        _ = Seq(coid00 -> 1, coid01 -> 0, coid10 -> 1).foreach { case (coid, deactivations) =>
          checkContractState(cd, coid, 0, 1 + deactivations, 0)(
            s"Contract $coid in creation is locked $deactivations times"
          )
        }

        // Finalize second request
        fin1 <- cd
          .finalizeRequest(mkCommitSet(arch = Set(coid00, coid10)), toc1)
          .flatten
          .failOnShutdown
        _ = fin1 shouldBe Right(())
        _ <- List(coid00 -> 0, coid10 -> 0).parTraverse_ { case (coid, deactivationLocks) =>
          checkContractState(cd, coid, Archived, toc1, 0, 1 + deactivationLocks, 0)(
            s"contract $coid archived by opportunistic follow-up still locked"
          )
          checkContractState(acs, coid, (Archived, toc1))(s"contract $coid is archived")
        }

        // Finalize first request
        _ = acs.setCreateHook { (_, _) =>
          Future.successful {
            checkContractState(cd, coid00, Archived, toc1, 0, 0, 1)(
              s"Contract $coid00 has a pending creation, but remains archived"
            )
            checkContractState(cd, coid10, Archived, toc1, 0, 0, 1)(
              s"Transient contract $coid10 has one pending writes."
            )
            checkContractState(cd, coid01, active, toc0, 0, 0, 1)(
              s"Contract $coid01 is being created."
            )
          }
        }
        commitSet0 = mkCommitSet(create = Set(coid00, coid01, coid10), arch = Set(coid10))
        fin0 <- cd.finalizeRequest(commitSet0, toc0).flatten.failOnShutdown
        _ = fin0 shouldBe Left(NonEmptyChain(AcsError(DoubleContractArchival(coid10, toc1, toc0))))
        _ <- checkContractState(acs, coid00, (Archived, toc1))(s"contract $coid00 remains archived")
        _ <- checkContractState(acs, coid01, (active, toc0))(s"contract $coid01 is active")
        _ <- checkContractState(acs, coid10, (Archived, toc1))(
          s"transient contract $coid10 is archived twice"
        )
      } yield succeed
    }

    "create a rolled back contract after it has been evicted" in {
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(1))
      for {
        acs <- mkAcs()
        cd = mkCd(acs)

        cr0 <- prefetchAndCheck(cd, RequestCounter(0), mkActivenessSet(create = Set(coid00)))
        _ = cr0 shouldBe mkActivenessResult()
        fin0 <- cd
          .finalizeRequest(CommitSet.empty, TimeOfChange(RequestCounter(0), Epoch))
          .flatten
          .failOnShutdown
        _ = assert(fin0 == Right(()))
        _ = checkContractStateAbsent(cd, coid00)(s"Rolled back contract $coid00 is evicted")

        // Re-creating rolled-back contract coid00
        cr1 <- prefetchAndCheck(cd, RequestCounter(1), mkActivenessSet(create = Set(coid00)))
        _ = cr1 shouldBe mkActivenessResult()
        fin1 <- cd.finalizeRequest(mkCommitSet(create = Set(coid00)), toc1).flatten.failOnShutdown
        _ = fin1 shouldBe Right(())
        _ <- checkContractState(acs, coid00, (active, toc1))(s"Contract $coid00 created")
      } yield succeed
    }

    "cannot create a rolled back contract before it is evicted" in {
      val cd = mkCd()
      for {
        cr0 <- prefetchAndCheck(cd, RequestCounter(0), mkActivenessSet(create = Set(coid00)))
        _ = cr0 shouldBe mkActivenessResult()

        cr1 <- prefetchAndCheck(cd, RequestCounter(1), mkActivenessSet(deact = Set(coid00)))
        _ = assert(cr1 == mkActivenessResult(locked = Set(coid00)))

        fin0 <- cd
          .finalizeRequest(CommitSet.empty, TimeOfChange(RequestCounter(0), Epoch))
          .flatten
          .failOnShutdown
        _ = assert(fin0 == Right(()))
        _ = checkContractState(cd, coid00, 0, 1, 0)(s"Rolled back contract $coid00 is locked")

        cr2 <- prefetchAndCheck(cd, RequestCounter(2), mkActivenessSet(create = Set(coid00)))
        _ = assert(
          cr2 == mkActivenessResult(locked = Set(coid00)),
          s"Rolled-back contract $coid00 cannot be re-created",
        )
      } yield succeed
    }

    "interleave ACS updates with further requests" in {
      val tocN1 = TimeOfChange(RequestCounter(-1), Epoch)
      val toc0 = TimeOfChange(RequestCounter(0), ofEpochMilli(1))
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(2))
      val toc3 = TimeOfChange(RequestCounter(3), ofEpochMilli(4))

      val actSet0 = mkActivenessSet(
        create = Set(coid10, coid11, coid20, coid22),
        deact = Set(coid00, coid01, coid21),
      )
      val actRes0 = mkActivenessResult(locked = Set(coid20, coid11, coid00, coid10))
      val commitSet0 = mkCommitSet(
        arch = Set(coid00, coid01, coid20, coid11),
        create = Set(coid10, coid20, coid22),
      )

      val actSet1 = mkActivenessSet(deact = Set(coid20, coid11, coid10, coid00))
      val commitSet1 = mkCommitSet(arch = Set(coid00, coid20, coid11))

      val actSet2 = mkActivenessSet(
        create = Set(coid10),
        deact = Set(coid00, coid21, coid20, coid11, coid22),
        prior = Set(coid21, coid22),
      )
      val actRes2 = mkActivenessResult(
        locked = Set(coid00, coid11, coid20),
        notFresh = Set(coid10),
        prior = Map(coid21 -> Some(active), coid22 -> Some(active)),
      )

      val actSet3 = mkActivenessSet(deact = Set(coid20))

      val actRes3 = mkActivenessResult(locked = Set(coid20))

      val finF1Complete = Promise[Unit]()

      def finalizeForthRequest(cd: ConflictDetector) =
        for {
          fin3 <- cd.finalizeRequest(mkCommitSet(arch = Set(coid20)), toc3).flatten.failOnShutdown
          _ <-
            finF1Complete.future // Delay ACs updates until outer finalizeRequest future has completed
        } yield {
          assert(fin3 == Right(()))
          checkContractState(cd, coid20, Archived, toc3, 0, 1, 2)(s"Contract $coid20 is archived")
          ()
        }

      val finF0Complete = Promise[Unit]()

      def storeHookRequest0(cd: ConflictDetector, acs: HookedAcs) = {
        // This runs while request 0's ACS updates are written
        checkContractState(cd, coid00, Archived, toc0, 0, 1, 1)(
          s"Contract $coid00 being archived remains locked."
        )
        checkContractState(cd, coid01, Archived, toc0, 0, 0, 1)(
          s"Contract $coid01 has a pending archival."
        )
        checkContractState(cd, coid10, active, toc0, 0, 1, 1)(
          s"Contract $coid10 has an opportunistic follow-up."
        )
        checkContractState(cd, coid11, Archived, toc0, 0, 1, 1)(
          s"Rolled-back transient contract $coid11 remains locked."
        )
        checkContractState(cd, coid20, Archived, toc0, 0, 2, 1)(
          s"Transient contract $coid20 remains locked twice."
        )
        checkContractStateAbsent(cd, coid21)(s"Contract $coid21 is evicted.")
        checkContractState(cd, coid22, active, toc0, 0, 0, 1)(s"Contract $coid22 is being created.")

        // Run another request while the updates are in flight
        for {
          // Activeness check for the third request
          cr2 <- prefetchAndCheck(cd, RequestCounter(2), actSet2)
          _ = assert(cr2 == actRes2)
          _ = checkContractState(cd, coid00, Archived, toc0, 0, 2, 1)(
            s"Contract $coid00 is locked once more."
          )
          _ = checkContractState(cd, coid10, active, toc0, 0, 1 + 1, 1)(
            s"Contract $coid10 in creation is locked for activation again."
          )
          _ = checkContractState(cd, coid11, Archived, toc0, 0, 2, 1)(
            s"contract $coid11 is locked once more."
          )
          _ = checkContractState(cd, coid20, Archived, toc0, 0, 3, 1)(
            s"Contract $coid20 is locked three times."
          )
          _ = checkContractState(cd, coid21, active, tocN1, 0, 1, 0)(s"Contract $coid21 is locked.")
          _ = checkContractState(cd, coid22, active, toc0, 0, 1, 1)(
            s"Created contract $coid22 is locked."
          )

          // Finalize the second request
          _ = acs.setArchiveHook((_, _) =>
            finalizeForthRequest(cd)
          ) // This runs while request 1's ACS updates are written
          finF1 <- cd.finalizeRequest(commitSet1, toc1).failOnShutdown
          _ = finF1Complete.success(())
          fin1 <- finF1.failOnShutdown
          _ = assert(
            fin1 == Left(NonEmptyChain(AcsError(DoubleContractArchival(coid20, toc3, toc1))))
          )
          _ = checkContractState(cd, coid00, Archived, toc1, 0, 1, 1)(
            s"Archived contract $coid00 remains locked."
          )
          _ = checkContractState(cd, coid10, active, toc0, 0, 1, 1)(
            s"Writing the creation for contract $coid10 is pending."
          )
          _ = checkContractState(cd, coid11, Archived, toc1, 0, 1, 1)(
            s"Archived contract $coid11 has still a pending write."
          )
          _ = checkContractState(cd, coid20, Archived, toc3, 0, 1, 1)(
            s"Contract $coid20 has its archival not updated."
          )
          _ <-
            finF0Complete.future // Delay ACS updates until the outer finalizeRequest future has completed
        } yield ()
      }

      for {
        rawAcs <- mkAcs(
          (coid00, tocN1, active),
          (coid01, tocN1, active),
          (coid21, tocN1, active),
        )
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        // Activeness check for first request
        cr0 <- prefetchAndCheck(cd, RequestCounter(0), actSet0)
        _ = cr0 shouldBe mkActivenessResult()

        // Activeness check for second request
        cr1 <- prefetchAndCheck(cd, RequestCounter(1), actSet1)
        _ = cr1 shouldBe actRes0

        _ = Seq(
          (coid00, Some((active, tocN1)), 0, 2),
          (coid01, Some((active, tocN1)), 0, 1),
          (coid10, None, 1, 1),
          (coid11, None, 1, 1),
          (coid20, None, 1, 1),
          (coid21, Some((active, tocN1)), 0, 1),
          (coid22, None, 1, 0),
        ).foreach {
          case (coid, Some((status, version)), activationLock, deactivationLock) =>
            checkContractState(cd, coid, status, version, 0, activationLock + deactivationLock, 0)(
              s"State of existing contract $coid"
            )
          case (coid, None, activationLock, deactivationLock) =>
            checkContractState(cd, coid, 0, activationLock + deactivationLock, 0)(
              s"State of nonexistent contract $coid"
            )
        }

        // Activeness check for fourth request
        cr3 <- prefetchAndCheck(cd, RequestCounter(3), actSet3)
        _ = cr3 shouldBe actRes3

        // Finalize the first request and do a lot of stuff while the updates are being written
        _ = acs.setArchiveHook((_, _) => storeHookRequest0(cd, acs))
        finF0 <- cd.finalizeRequest(commitSet0, toc0).failOnShutdown
        _ = finF0Complete.success(())
        fin0 <- finF0.failOnShutdown
        _ =
          fin0.leftMap(_.toList.toSet) shouldBe Left(
            Set(
              AcsError(DoubleContractArchival(coid00, toc1, toc0)),
              AcsError(DoubleContractArchival(coid11, toc1, toc0)),
              AcsError(DoubleContractArchival(coid20, toc1, toc0)),
            )
          )
        _ = checkContractState(cd, coid00, Archived, toc1, 0, 1, 0)(
          s"Contract $coid00 remains locked."
        )
        _ = checkContractStateAbsent(cd, coid01)(s"Contract $coid01 has been evicted.")
        _ = checkContractState(cd, coid10, active, toc0, 0, 1, 0)(
          s"Contract $coid10 remains locked."
        )
        _ = checkContractState(cd, coid11, Archived, toc1, 0, 1, 0)(
          s"Contract $coid11 remains locked."
        )
        _ = checkContractState(cd, coid20, Archived, toc3, 0, 1, 0)(
          s"Contract $coid20 remains locked."
        )
        _ = checkContractState(cd, coid21, active, tocN1, 0, 1, 0)(
          s"Contract $coid21 remains locked."
        )
        _ = checkContractState(cd, coid22, active, toc0, 0, 1, 0)(
          s"Contract $coid22 remains locked."
        )
      } yield succeed
    }

    "transfer-in unknown contracts" in {
      for {
        acs <- mkAcs()
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(acs, transferCache)
        ts = ofEpochMilli(1)
        actSet = mkActivenessSet(
          tfIn = Set(coid00, coid01, coid10),
          transferIds = Set(transfer1),
        ) // omit transfer2 to mimick a non-transferring participant
        tfIn <- prefetchAndCheck(cd, RequestCounter(0), actSet)
        _ = tfIn shouldBe mkActivenessResult()
        _ = Seq(coid00, coid01, coid10).foreach { coid =>
          checkContractState(cd, coid00, 0, 1, 0)(s"Contract $coid is locked for activation.")
        }
        commitSet = mkCommitSet(tfIn = Map(coid00 -> transfer1, coid01 -> transfer2))
        toc = TimeOfChange(RequestCounter(0), ts)
        finTxIn <- cd.finalizeRequest(commitSet, toc).flatten.failOnShutdown
        _ = assert(finTxIn == Right(()))
        _ = Seq(coid00, coid01, coid10).foreach { coid =>
          checkContractStateAbsent(cd, coid)(s"Contract $coid is evicted.")
        }
        fetch00 <- acs.fetchState(coid00)
        fetch01 <- acs.fetchState(coid01)
        fetch10 <- acs.fetchState(coid10)
        lookup1 <- transferCache.lookup(transfer1).value
        lookup2 <- transferCache.lookup(transfer2).value
      } yield {
        assert(
          fetch00.contains(AcsContractState(active, RequestCounter(0), ts)),
          s"Contract $coid00 is active.",
        )
        assert(
          fetch01.contains(AcsContractState(active, RequestCounter(0), ts)),
          s"Contract $coid01 is active.",
        )
        assert(fetch10.isEmpty, s"Contract $coid10 remains unknown.")
        assert(lookup1 == Left(TransferCompleted(transfer1, toc)), s"$transfer1 completed")
        assert(lookup2.exists(_.transferId == transfer2), s"$transfer2 has not been completed")
      }
    }

    "transfer-in a known contract" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs(
          (coid00, toc0, Archived),
          (coid01, toc0, TransferredAway(targetDomain1, initialTransferCounter)),
        )
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1
        ) // Omit transfer2 to mimic a non-transferring participant
        cd = mkCd(acs, transferCache)
        ts = ofEpochMilli(1)
        toc1 = TimeOfChange(RequestCounter(1), ts)
        actSet = mkActivenessSet(
          tfIn = Set(coid00, coid01),
          transferIds = Set(transfer1),
          prior = Set(coid00, coid01),
        )
        commitSet = mkCommitSet(tfIn = Map(coid00 -> transfer1, coid01 -> transfer2))
        actRes <- prefetchAndCheck(cd, RequestCounter(1), actSet)
        fin <- cd.finalizeRequest(commitSet, toc1).flatten.failOnShutdown
        fetch00 <- acs.fetchState(coid00)
        fetch01 <- acs.fetchState(coid01)
        lookup1 <- transferCache.lookup(transfer1).value
        lookup2 <- transferCache.lookup(transfer2).value
      } yield {
        assert(
          actRes == mkActivenessResult(
            notFree = Map(coid00 -> Archived),
            prior = Map(
              coid00 -> Some(Archived),
              coid01 -> Some(TransferredAway(targetDomain1, initialTransferCounter)),
            ),
          ),
          s"Report that $coid00 was already archived.",
        )
        assert(
          fin == Left(NonEmptyChain(AcsError(ChangeAfterArchival(coid00, toc0, toc1)))),
          s"Report transfer-in afeter archival.",
        )
        assert(
          fetch00.contains(AcsContractState(active, RequestCounter(1), ts)),
          s"Contract $coid00 is transferred in.",
        )
        assert(
          fetch01.contains(AcsContractState(active, RequestCounter(1), ts)),
          s"Contract $coid01 is transferred in.",
        )
        assert(lookup1 == Left(TransferCompleted(transfer1, toc1)), s"$transfer1 completed")
        assert(lookup2 == Left(UnknownTransferId(transfer2)), s"$transfer2 does not exist")
      }
    }

    "transfer-out several contracts" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs((coid00, toc0, active), (coid01, toc0, active))
        cd = mkCd(acs)
        activenessSet = mkActivenessSet(deact = Set(coid00, coid01), prior = Set(coid00, coid01))
        actRes = mkActivenessResult(prior = Map(coid00 -> Some(active), coid01 -> Some(active)))
        commitSet = mkCommitSet(tfOut =
          Map(coid00 -> (domain1 -> transferCounter1), coid01 -> (domain2 -> transferCounter2))
        )
        ts = ofEpochMilli(1)
        _ <- singleCRwithTR(cd, RequestCounter(1), activenessSet, actRes, commitSet, ts)
        fetch00 <- acs.fetchState(coid00)
        fetch01 <- acs.fetchState(coid01)
      } yield {
        assert(
          fetch00.contains(
            AcsContractState(
              TransferredAway(targetDomain1, transferCounter1),
              RequestCounter(1),
              ts,
            )
          ),
          s"Contract $coid00 transferred to $sourceDomain1.",
        )
        assert(
          fetch01.contains(
            AcsContractState(
              TransferredAway(targetDomain2, transferCounter2),
              RequestCounter(1),
              ts,
            )
          ),
          s"Contract $coid01 transferred to $domain2.",
        )
      }
    }

    "mix transfers with creations and archivals" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs(
          (coid00, toc0, active),
          (coid11, toc0, active),
        )
        _ <- acs.transferInContract(coid01, toc0, sourceDomain1, transferCounter1).value
        transferCache <- mkTransferCache(loggerFactory)(transfer2 -> mediator2)
        cd = mkCd(acs, transferCache)
        activenessSet = mkActivenessSet(
          deact = Set(coid00, coid01),
          create = Set(coid10),
          tfIn = Set(coid20),
          transferIds = Set(transfer2),
          useOnly = Set(coid11),
          prior = Set(coid01, coid00, coid11),
        )
        actRes = mkActivenessResult(prior =
          Map(
            coid00 -> Some(active),
            coid01 -> Some(Active(transferCounter1)),
            coid11 -> Some(active),
          )
        )
        commitSet = mkCommitSet(
          arch = Set(coid00),
          tfOut = Map(coid01 -> (domain1 -> transferCounter2)),
          tfIn = Map(coid20 -> transfer2),
          create = Set(coid10),
        )
        ts = ofEpochMilli(1)
        _ <- singleCRwithTR(cd, RequestCounter(1), activenessSet, actRes, commitSet, ts)
        fetch00 <- acs.fetchState(coid00)
        fetch01 <- acs.fetchState(coid01)
        fetch10 <- acs.fetchState(coid10)
        fetch11 <- acs.fetchState(coid11)
        fetch20 <- acs.fetchState(coid20)
        lookup2 <- transferCache.lookup(transfer2).value
      } yield {
        assert(
          fetch00.contains(AcsContractState(Archived, RequestCounter(1), ts)),
          s"Contract $coid00 is archived.",
        )
        assert(
          fetch01.contains(
            AcsContractState(
              TransferredAway(targetDomain1, transferCounter2),
              RequestCounter(1),
              ts,
            )
          ),
          s"Contract $coid01 is transferred to $targetDomain1.",
        )
        assert(
          fetch10.contains(AcsContractState(active, RequestCounter(1), ts)),
          s"Contract $coid10 is created.",
        )
        assert(
          fetch11.contains(AcsContractState(active, RequestCounter(0), Epoch)),
          s"Contract $coid11 remains active.",
        )
        assert(
          fetch20.contains(AcsContractState(active, RequestCounter(1), ts)),
          s"Contract $coid20 is transferred in.",
        )
        assert(
          lookup2 == Left(TransferCompleted(transfer2, TimeOfChange(RequestCounter(1), ts))),
          s"$transfer2 completed",
        )
      }
    }

    "allow repurposing the activation locks" in {
      for {
        acs <- mkAcs()
        transferCache <- mkTransferCache(loggerFactory)(transfer1 -> mediator1)
        cd = mkCd(acs, transferCache)
        activenessSet = mkActivenessSet(
          create = Set(coid00),
          tfIn = Set(coid01),
          transferIds = Set(transfer1),
        )
        commitSet = mkCommitSet(create = Set(coid01), tfIn = Map(coid00 -> transfer1))
        _ <- singleCRwithTR(
          cd,
          RequestCounter(0),
          activenessSet,
          mkActivenessResult(),
          commitSet,
          Epoch,
        )
        fetch00 <- acs.fetchState(coid00)
        fetch01 <- acs.fetchState(coid01)
      } yield {
        assert(
          fetch00.contains(AcsContractState(active, RequestCounter(0), Epoch)),
          s"Contract $coid00 is transferred in.",
        )
        assert(
          fetch01.contains(AcsContractState(active, RequestCounter(0), Epoch)),
          s"Contract $coid01 is created.",
        )
      }
    }

    "support transient contracts with transfers" in {
      for {
        acs <- mkAcs()
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(acs, transferCache)
        activenessSet = mkActivenessSet(
          tfIn = Set(coid10, coid11),
          transferIds = Set(transfer1, transfer2),
          create = Set(coid20),
        )
        commitSet = mkCommitSet(
          create = Set(coid20),
          tfIn = Map(coid10 -> transfer2, coid11 -> transfer1),
          tfOut =
            Map(coid20 -> (domain1 -> transferCounter1), coid11 -> (domain2 -> transferCounter2)),
          arch = Set(coid10),
        )
        _ <- singleCRwithTR(
          cd,
          RequestCounter(0),
          activenessSet,
          mkActivenessResult(),
          commitSet,
          Epoch,
        )
        fetch10 <- acs.fetchState(coid10)
        fetch11 <- acs.fetchState(coid11)
        fetch20 <- acs.fetchState(coid20)
      } yield {
        assert(
          fetch10.contains(AcsContractState(Archived, RequestCounter(0), Epoch)),
          s"Contract $coid10 is archived",
        )
        assert(
          fetch11.contains(
            AcsContractState(
              TransferredAway(targetDomain2, transferCounter2),
              RequestCounter(0),
              Epoch,
            )
          ),
          s"Contract $coid11 is transferred to $targetDomain2",
        )
        assert(
          fetch20.contains(
            AcsContractState(
              TransferredAway(targetDomain1, transferCounter1),
              RequestCounter(0),
              Epoch,
            )
          ),
          s"Contract $coid20 is transferred to $targetDomain1",
        )
      }
    }

    "double spend a transferred-away contract" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs((coid00, toc0, TransferredAway(targetDomain1, transferCounter1)))
        cd = mkCd(acs)
        actRes1 <- prefetchAndCheck(
          cd,
          RequestCounter(1),
          mkActivenessSet(deact = Set(coid00), prior = Set(coid00)),
        )
        fin1 <- cd
          .finalizeRequest(
            mkCommitSet(tfOut = Map(coid00 -> (domain2 -> transferCounter2))),
            TimeOfChange(RequestCounter(1), ofEpochMilli(1)),
          )
          .flatten
          .failOnShutdown
        fetch00 <- acs.fetchState(coid00)
      } yield {
        assert(
          actRes1 == mkActivenessResult(
            notActive = Map(coid00 -> TransferredAway(targetDomain1, transferCounter1)),
            prior = Map(coid00 -> Some(TransferredAway(targetDomain1, transferCounter1))),
          )
        )
        assert(fin1 == Right(()))
        assert(
          fetch00.contains(
            AcsContractState(
              TransferredAway(targetDomain2, transferCounter2),
              RequestCounter(1),
              ofEpochMilli(1),
            )
          )
        )
      }
    }

    "double transfer-in a contract" in {
      for {
        acs <- mkAcs()
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(acs, transferCache)
        actSet1 = mkActivenessSet(tfIn = Set(coid00), transferIds = Set(transfer1))
        actSet2 = mkActivenessSet(tfIn = Set(coid00), transferIds = Set(transfer2))
        commitSet1 = mkCommitSet(tfIn = Map(coid00 -> transfer1))
        commitSet2 = mkCommitSet(tfIn = Map(coid00 -> transfer2))
        _ <- singleCRwithTR(
          cd,
          RequestCounter(0),
          actSet1,
          mkActivenessResult(),
          commitSet1,
          Epoch,
        )
        actRes2 <- prefetchAndCheck(cd, RequestCounter(1), actSet2)
        fin2 <- cd
          .finalizeRequest(commitSet2, TimeOfChange(RequestCounter(1), ofEpochMilli(1000)))
          .flatten
          .failOnShutdown
        fetch00 <- acs.fetchState(coid00)
      } yield {
        assert(
          actRes2 == mkActivenessResult(notFree = Map(coid00 -> active)),
          s"double activation is reported",
        )
        assert(fin2 == Right(()))
        assert(fetch00.contains(AcsContractState(active, RequestCounter(1), ofEpochMilli(1000))))
      }
    }

    "handle double activations and double deactivations at the same timestamp" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      for {
        acs <- mkAcs((coid00, toc0, active), (coid01, toc0, active))
        transferCache <- mkTransferCache(loggerFactory)(transfer2 -> mediator2)
        cd = mkCd(acs, transferCache)
        actSet1 = mkActivenessSet(create = Set(coid10), deact = Set(coid00, coid01))
        commitSet1 = mkCommitSet(
          create = Set(coid10),
          arch = Set(coid00),
          tfOut = Map(coid01 -> (domain1 -> transferCounter1)),
        )
        actSet2 = mkActivenessSet(
          tfIn = Set(coid10),
          deact = Set(coid00, coid01),
          transferIds = Set(transfer2),
        )
        commitSet2 = mkCommitSet(
          tfIn = Map(coid10 -> transfer2),
          tfOut =
            Map(coid00 -> (domain2 -> transferCounter1), coid01 -> (domain2 -> transferCounter2)),
        )
        ts = ofEpochMilli(1000)
        toc2 = TimeOfChange(RequestCounter(2), ts)
        toc1 = TimeOfChange(RequestCounter(1), ts)
        actRes1 <- prefetchAndCheck(cd, RequestCounter(1), actSet1)
        _ = assert(actRes1 == mkActivenessResult())
        actRes2 <- prefetchAndCheck(cd, RequestCounter(2), actSet2)
        _ = assert(actRes2 == mkActivenessResult(locked = Set(coid00, coid01, coid10)))
        fin2 <- cd.finalizeRequest(commitSet2, toc2).flatten.failOnShutdown
        fin1 <- cd.finalizeRequest(commitSet1, toc1).flatten.failOnShutdown
      } yield {
        assert(fin2 == Right(()), s"First commit goes through")
        fin1
          .leftOrFail("Double (de)activations are reported.")
          .toList should contain(AcsError(ChangeAfterArchival(coid00, toc1, toc2)))

      }
    }

    "detect contract conflicts between transfer-ins" in {
      for {
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(transferCache = transferCache)
        actRes1 <- prefetchAndCheck(
          cd,
          RequestCounter(0),
          mkActivenessSet(tfIn = Set(coid00), transferIds = Set(transfer1)),
        )
        actRes2 <- prefetchAndCheck(
          cd,
          RequestCounter(1),
          mkActivenessSet(tfIn = Set(coid00), transferIds = Set(transfer2)),
        )
      } yield {
        assert(actRes1 == mkActivenessResult())
        assert(actRes2 == mkActivenessResult(locked = Set(coid00)))
        checkContractState(cd, coid00, 0, 2, 0)(s"activation lock held twice")
      }
    }

    "detect conflicts between transfer-ins and creates" in {
      for {
        transferCache <- mkTransferCache(loggerFactory)(
          transfer1 -> mediator1,
          transfer2 -> mediator2,
        )
        cd = mkCd(transferCache = transferCache)
        actSet1 = mkActivenessSet(
          tfIn = Set(coid00),
          create = Set(coid01),
          transferIds = Set(transfer1),
        )
        actRes1 <- prefetchAndCheck(cd, RequestCounter(0), actSet1)
        actSet2 = mkActivenessSet(
          tfIn = Set(coid01),
          create = Set(coid00),
          transferIds = Set(transfer2),
        )
        actRes2 <- prefetchAndCheck(cd, RequestCounter(1), actSet2)
      } yield {
        assert(actRes1 == mkActivenessResult())
        assert(actRes2 == mkActivenessResult(locked = Set(coid01, coid00)))
      }
    }

    "detect conflicts between racing transfer-ins" in {
      val transferStore = new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory)
      val hookedStore = new TransferCacheTest.HookTransferStore(transferStore)
      for {
        transferCache <- mkTransferCache(loggerFactory, hookedStore)(transfer1 -> mediator1)
        cd = mkCd(transferCache = transferCache)
        actSet = mkActivenessSet(tfIn = Set(coid00), transferIds = Set(transfer1))
        _actRes <- prefetchAndCheck(cd, RequestCounter(0), actSet)
        commitSet = mkCommitSet(tfIn = Map(coid00 -> transfer1))
        toc = TimeOfChange(RequestCounter(0), ofEpochMilli(1))
        toc2 = TimeOfChange(RequestCounter(2), ofEpochMilli(3))
        promise = Promise[Either[NonEmptyChain[RequestTracker.RequestTrackerStoreError], Unit]]()
        _ = hookedStore.preComplete { (_transferId, _toc) =>
          // This runs after committing the request, but before the transfer store is updated
          val actSetOut = mkActivenessSet(deact = Set(coid00))
          val commitSetOut = mkCommitSet(tfOut = Map(coid00 -> (domain1 -> transferCounter1)))
          CheckedT(for {
            _ <- singleCRwithTR(
              cd,
              RequestCounter(1),
              actSetOut,
              mkActivenessResult(),
              commitSetOut,
              ofEpochMilli(2),
            )
            actRes2 <- prefetchAndCheck(cd, RequestCounter(2), actSet)
            _ = promise.completeWith(cd.finalizeRequest(commitSet, toc2).flatten.failOnShutdown)
          } yield {
            assert(
              actRes2 == mkActivenessResult(inactiveTransfers = Set(transfer1)),
              s"Double transfer-in $transfer1",
            )
            Checked.result(())
          })
        }
        fin1 <- cd.finalizeRequest(commitSet, toc).flatten.failOnShutdown
        fin2 <- promise.future
      } yield {
        assert(fin1 == Right(()), "First transfer-in succeeds")
        fin2.leftOrFail(s"Transfer $transfer1 was already completed").toList should contain(
          TransferStoreError(TransferAlreadyCompleted(transfer1, toc2))
        )
      }
    }

    "work with pruning" in {
      val toc0 = TimeOfChange(RequestCounter(0), Epoch)
      val toc1 = TimeOfChange(RequestCounter(1), ofEpochMilli(1))
      val toc2 = TimeOfChange(RequestCounter(2), ofEpochMilli(2))
      val toc3 = TimeOfChange(RequestCounter(3), ofEpochMilli(3))
      implicit val closeContext: CloseContext = HasTestCloseContext.makeTestCloseContext(logger)
      for {
        acs <- mkAcs((coid00, toc0, active), (coid01, toc0, active))
        cd = mkCd(acs)

        actSet1 = mkActivenessSet(deact = Set(coid00, coid01))
        cr1 <- prefetchAndCheck(cd, RequestCounter(1), actSet1)
        _ = assert(cr1 == mkActivenessResult())

        actSet2 = mkActivenessSet(deact = Set(coid00, coid01))
        cr2 <- prefetchAndCheck(cd, RequestCounter(2), actSet2)
        _ = assert(
          cr2 == mkActivenessResult(locked = Set(coid00, coid01))
        )

        actSet3 = actSet2
        cr2 <- prefetchAndCheck(cd, RequestCounter(3), actSet3)
        _ = assert(
          cr2 == mkActivenessResult(locked = Set(coid00, coid01))
        )

        commitSet1 = mkCommitSet(
          arch = Set(coid00),
          tfOut = Map(coid01 -> (domain1 -> transferCounter1)),
        )
        fin1 <- cd.finalizeRequest(commitSet1, toc1).flatten.failOnShutdown
        _ = assert(fin1 == Right(()))

        _ <- acs.prune(toc1.timestamp)

        _ <- checkContractState(acs, coid00, None)(s"$coid00 has been pruned")
        _ <- checkContractState(acs, coid01, None)(s"$coid01 has been pruned")

        // Triggers invariant checking if invariant checking is enabled
        fin2 <- cd.finalizeRequest(CommitSet.empty, toc2).flatten.failOnShutdown

        // Triggers invariant checking if invariant checking is enabled
        fin3 <- cd.finalizeRequest(CommitSet.empty, toc3).flatten.failOnShutdown
      } yield {
        assert(fin2 == Right(()))
        assert(fin3 == Right(()))
      }
    }

    "interleave pre-fetching" in {
      val tocN1 = TimeOfChange(RequestCounter(-1), Epoch)
      val actSet0 = mkActivenessSet(create = Set(coid10), deact = Set(coid00, coid01))
      val actSet1 = mkActivenessSet(deact = Set(coid21))

      def setFetchHook(acs: HookedAcs, cd: ConflictDetector): Unit =
        acs.setFetchHook { _contractIds =>
          // This runs while the contracts for the first request are prefetched from the ACS
          // Prefetch second request with distinct contracts
          cd.registerActivenessSet(RequestCounter(1), actSet1).failOnShutdown
        }

      for {
        rawAcs <- mkAcs(
          (coid00, tocN1, active),
          (coid01, tocN1, active),
          (coid21, tocN1, active),
        )
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        _ = setFetchHook(acs, cd)
        _ <- cd.registerActivenessSet(RequestCounter(0), actSet0).failOnShutdown

        // Activeness check for the first request
        cr0 <- cd.checkActivenessAndLock(RequestCounter(0)).failOnShutdown
        _ = assert(cr0 == mkActivenessResult())

        // Activeness check for second request
        cr1 <- cd.checkActivenessAndLock(RequestCounter(1)).failOnShutdown
        _ = assert(cr1 == mkActivenessResult())

        _ = Seq(
          (coid00, Some((active, tocN1)), 0, 1),
          (coid01, Some((active, tocN1)), 0, 1),
          (coid10, None, 1, 0),
          (coid21, Some((active, tocN1)), 0, 1),
        ).foreach {
          case (coid, Some((status, version)), activationLock, deactivationLock) =>
            checkContractState(cd, coid, status, version, 0, activationLock + deactivationLock, 0)(
              s"State of existing contract $coid"
            )
          case (coid, None, activationLock, deactivationLock) =>
            checkContractState(cd, coid, 0, activationLock + deactivationLock, 0)(
              s"State of nonexistent contract $coid"
            )
        }
      } yield succeed
    }

    "detect non-prefetched states" in {
      def setFetchHook(acs: HookedAcs, cd: ConflictDetector): Unit =
        acs.setFetchHook { _contractIds =>
          // This runs while the contracts for the request are prefetched from the ACS
          // Trigger checking already now to provoke an error
          cd.checkActivenessAndLock(RequestCounter(0)).void.failOnShutdown
        }

      for {
        rawAcs <- mkAcs()
        acs = new HookedAcs(rawAcs)(parallelExecutionContext)
        cd = mkCd(acs)

        _ = setFetchHook(acs, cd)
        _ <- loggerFactory.assertThrowsAndLogsAsync[ConflictDetectionStoreAccessError](
          cd.registerActivenessSet(
            RequestCounter(0),
            mkActivenessSet(create = Set(coid00, coid01)),
          ).failOnShutdown,
          _ => succeed,
          entry => {
            entry.errorMessage should include("An internal error has occurred.")
            val cause = entry.throwable.value
            cause shouldBe a[IllegalConflictDetectionStateException]
            cause.getMessage should include(s"Request 0 has outstanding pre-fetches:")
          },
        )
      } yield succeed
    }
  }

  def checkContractState(acs: ActiveContractStore, coid: LfContractId, cs: (Status, TimeOfChange))(
      clue: String
  ): Future[Assertion] =
    checkContractState(acs, coid, Some(AcsContractState(cs._1, cs._2)))(clue)

  def checkContractState(
      acs: ActiveContractStore,
      coid: LfContractId,
      state: Option[AcsContractState],
  )(clue: String): Future[Assertion] =
    acs.fetchState(coid).map(result => assert(result == state, clue))

  private[this] def mkState[A <: PrettyPrinting](
      state: Option[StateChange[A]],
      pendingActivenessCount: Int,
      locks: Int,
      pendingWriteCount: Int,
  ): ImmutableLockableState[A] =
    ImmutableContractState(
      Some(state),
      PendingActivenessCheckCounter.assertFromInt(pendingActivenessCount),
      LockCounter.assertFromInt(locks),
      PendingWriteCounter.assertFromInt(pendingWriteCount),
    )

  private def checkContractState(
      cd: ConflictDetector,
      coid: LfContractId,
      status: Status,
      version: TimeOfChange,
      pendingActivenessCount: Int,
      locks: Int,
      pendingWriteCount: Int,
  )(clue: String): Assertion = {
    val expected = mkState(
      Some(ActiveContractStore.ContractState(status, version)),
      pendingActivenessCount,
      locks,
      pendingWriteCount,
    )
    assert(cd.getInternalContractState(coid).contains(expected), clue)
  }

  private def checkContractState(
      cd: ConflictDetector,
      coid: LfContractId,
      pendingActivenessCount: Int,
      locks: Int,
      pendingWriteCount: Int,
  )(clue: String): Assertion = {
    val expected = mkState(None, pendingActivenessCount, locks, pendingWriteCount)
    assert(cd.getInternalContractState(coid).contains(expected), clue)
  }

  private def checkContractStateAbsent(cd: ConflictDetector, coid: LfContractId)(clue: String)(
      implicit pos: source.Position
  ): Assertion =
    assert(cd.getInternalContractState(coid).isEmpty, clue)

  private def prefetchAndCheck(
      cd: ConflictDetector,
      rc: RequestCounter,
      activenessSet: ActivenessSet,
  ): Future[ActivenessResult] =
    cd.registerActivenessSet(rc, activenessSet)
      .flatMap(_ => cd.checkActivenessAndLock(rc))
      .failOnShutdown

  private def singleCRwithTR(
      cd: ConflictDetector,
      rc: RequestCounter,
      activenessSet: ActivenessSet,
      activenessResult: ActivenessResult,
      commitSet: CommitSet,
      recordTime: CantonTimestamp,
  ): Future[Assertion] =
    for {
      cr <- prefetchAndCheck(cd, rc, activenessSet)
      fin <- cd.finalizeRequest(commitSet, TimeOfChange(rc, recordTime)).flatten.failOnShutdown
    } yield {
      assert(cr == activenessResult, "activeness check reports the correct result")
      assert(fin == Right(()))
    }
}

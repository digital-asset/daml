// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.data.Ref.{Identifier, SimpleString}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  ValueText,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.backend.api.v1.RejectionReason
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{Contract, PostgresLedgerDao}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  TransactionSerializer,
  ValueSerializer,
  KeyHasher
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
//TODO: use scalacheck when we have generators available for contracts and transactions
class PostgresDaoSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with PostgresAroundAll
    with PropertyChecks {

  private lazy val dbDispatcher = DbDispatcher(postgresFixture.jdbcUrl, 4, 4)

  private lazy val ledgerDao =
    PostgresLedgerDao(
      dbDispatcher,
      ContractSerializer,
      TransactionSerializer,
      ValueSerializer,
      KeyHasher)

  private val nextOffset: () => Long = {
    val counter = new AtomicLong(0)
    () =>
      counter.getAndIncrement()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(ledgerDao.storeInitialLedgerEnd(0), Duration.Inf)
  }

  "Postgres Ledger DAO" should {
    "be able to persist and load contracts" in {
      val offset = nextOffset()
      val absCid = AbsoluteContractId("cId1")
      val let = Instant.now
      val contractInstance = ContractInst(
        Identifier(
          Ref.PackageId.assertFromString("packageId"),
          Ref.QualifiedName(
            Ref.ModuleName.assertFromString("moduleName"),
            Ref.DottedName.assertFromString("name"))),
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some text")),
        "agreement"
      )
      val keyWithMaintainers = KeyWithMaintainers(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key")),
        Set(Ref.Party.assertFromString("Alice"))
      )

      val contract = Contract(
        absCid,
        let,
        "trId1",
        "workflowId",
        Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
        contractInstance,
        Some(keyWithMaintainers)
      )

      val transaction = LedgerEntry.Transaction(
        "commandId1",
        "trId1",
        "appID1",
        "Alice",
        "workflowId",
        let,
        let,
        GenTransaction(
          Map(
            "event1" -> NodeCreate(
              absCid,
              contractInstance,
              None,
              Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
              Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
              Some(keyWithMaintainers)
            )),
          ImmArray("event1")
        ),
        Map("event1" -> Set("Alice", "Bob"), "event2" -> Set("Alice", "In", "Chains"))
      )

      for {
        result1 <- ledgerDao.lookupActiveContract(absCid)
        _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, transaction)
        result2 <- ledgerDao.lookupActiveContract(absCid)
      } yield {
        result1 shouldEqual None
        result2 shouldEqual Some(contract)
      }
    }

    "be able to persist and load a checkpoint" in {
      val checkpoint = LedgerEntry.Checkpoint(Instant.now)
      val offset = nextOffset()

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, checkpoint)
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual (Some(checkpoint))
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    val rejectionReasonGen: Gen[RejectionReason] = for {
      const <- Gen.oneOf[String => RejectionReason](
        Seq[String => RejectionReason](
          RejectionReason.Inconsistent.apply(_),
          RejectionReason.OutOfQuota.apply(_),
          RejectionReason.TimedOut.apply(_),
          RejectionReason.Disputed.apply(_),
          RejectionReason.DuplicateCommandId.apply(_)
        ))
      desc <- Arbitrary.arbitrary[String].filter(_.nonEmpty)
    } yield const(desc)

    "be able to persist and load a rejection" in {
      forAll(rejectionReasonGen) { rejectionReason =>
        val offset = nextOffset()
        val rejection = LedgerEntry.Rejection(
          Instant.now,
          s"commandId-$offset",
          s"applicationId-$offset",
          "party",
          rejectionReason)

        @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
        implicit val ec = DirectExecutionContext

        val resultF = for {
          startingOffset <- ledgerDao.lookupLedgerEnd()
          _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, rejection)
          entry <- ledgerDao.lookupLedgerEntry(offset)
          endingOffset <- ledgerDao.lookupLedgerEnd()
        } yield {
          entry shouldEqual Some(rejection)
          endingOffset shouldEqual (startingOffset + 1)
        }

        Await.result(resultF, Duration.Inf)
      }
    }

    "be able to persist and load a transaction" in {
      val offset = nextOffset()
      val absCid = AbsoluteContractId("cId2")
      val let = Instant.now
      val contractInstance = ContractInst(
        Identifier(
          Ref.PackageId.assertFromString("packageId"),
          Ref.QualifiedName(
            Ref.ModuleName.assertFromString("moduleName"),
            Ref.DottedName.assertFromString("name"))),
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some text")),
        "agreement"
      )

      val keyWithMaintainers = KeyWithMaintainers(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key2")),
        Set(Ref.Party.assertFromString("Alice"))
      )

      val contract = Contract(
        absCid,
        let,
        "trId2",
        "workflowId",
        Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
        contractInstance,
        Some(keyWithMaintainers)
      )

      val transaction = LedgerEntry.Transaction(
        "commandId2",
        "trId2",
        "appID2",
        "Alice",
        "workflowId",
        let,
        let,
        GenTransaction(
          Map(
            "event1" -> NodeCreate(
              absCid,
              contractInstance,
              None,
              Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
              Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
              Some(keyWithMaintainers)
            )),
          ImmArray("event1")
        ),
        Map("event1" -> Set("Alice", "Bob"), "event2" -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, transaction)
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(transaction)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to produce a valid snapshot" in {
      val templateId = Identifier(
        Ref.PackageId.assertFromString("packageId"),
        Ref.QualifiedName(
          Ref.ModuleName.assertFromString("moduleName"),
          Ref.DottedName.assertFromString("name")))

      def genCreateTransaction(id: Long) = {
        val txId = s"trId$id"
        val absCid = AbsoluteContractId(s"cId$id")
        val let = Instant.now
        val contractInstance = ContractInst(
          templateId,
          VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some text")),
          "agreement"
        )
        val contract = Contract(
          absCid,
          let,
          txId,
          "workflowId",
          Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
          contractInstance,
          None
        )

        LedgerEntry.Transaction(
          s"commandId$id",
          txId,
          "appID1",
          "Alice",
          "workflowId",
          let,
          let,
          GenTransaction(
            Map(
              s"event$id" -> NodeCreate(
                absCid,
                contractInstance,
                None,
                Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
                Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
                None
              )),
            ImmArray(s"event$id")
          ),
          Map(s"event$id" -> Set("Alice", "Bob"))
        )
      }

      def genExerciseTransaction(id: Long, targetCid: AbsoluteContractId) = {
        val txId = s"trId$id"
        val absCid = AbsoluteContractId(s"cId$id")
        val let = Instant.now
        LedgerEntry.Transaction(
          s"commandId$id",
          txId,
          "appID1",
          "Alice",
          "workflowId",
          let,
          let,
          GenTransaction(
            Map(
              s"event$id" -> NodeExercises(
                targetCid,
                templateId,
                "choice",
                None,
                true,
                Set(SimpleString.assertFromString("Alice")),
                VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some choice value")),
                Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
                Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
                Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
                ImmArray.empty
              )),
            ImmArray(s"event$id")
          ),
          Map(s"event$id" -> Set("Alice", "Bob"))
        )
      }

      def storeCreateTransaction() = {
        val offset = nextOffset()
        val t = genCreateTransaction(offset)
        ledgerDao.storeLedgerEntry(offset, offset + 1, t).map(_ => ())
      }

      def storeExerciseTransaction(targetCid: AbsoluteContractId) = {
        val offset = nextOffset()
        val t = genExerciseTransaction(offset, targetCid)
        ledgerDao.storeLedgerEntry(offset, offset + 1, t).map(_ => ())
      }

      val sumSink = Sink.fold[Int, Int](0)(_ + _)
      val N = 1000
      val M = 10

      def runSequentially(n: Int, f: Int => Future[Unit]) =
        Source(1 to n).mapAsync(1)(f).runWith(Sink.ignore)

      // Perform the following operations:
      // - Create N contracts
      // - Archive 1 contract
      // - Take a snapshot
      // - Create another M contracts
      // The resulting snapshot should contain N-1 contracts
      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        startingSnapshot <- ledgerDao.getActiveContractSnapshot()
        _ <- runSequentially(N, _ => storeCreateTransaction())
        _ <- storeExerciseTransaction(AbsoluteContractId(s"cId$startingOffset"))
        snapshotOffset <- ledgerDao.lookupLedgerEnd()
        snapshot <- ledgerDao.getActiveContractSnapshot()
        _ <- runSequentially(M, _ => storeCreateTransaction())
        endingOffset <- ledgerDao.lookupLedgerEnd()
        startingSnapshotSize <- startingSnapshot.acs.map(t => 1).runWith(sumSink)
        snapshotSize <- snapshot.acs.map(t => 1).runWith(sumSink)
      } yield {
        withClue("starting offset: ") {
          startingSnapshot.offset shouldEqual startingOffset
        }
        withClue("snapshot offset: ") {
          snapshot.offset shouldEqual snapshotOffset
        }
        withClue("snapshot offset (2): ") {
          snapshotOffset shouldEqual (startingOffset + N + 1)
        }
        withClue("ending offset: ") {
          endingOffset shouldEqual (snapshotOffset + M)
        }
        withClue("snapshot size: ") {
          (snapshotSize - startingSnapshotSize) shouldEqual (N - 1)
        }
      }
    }

  }

}

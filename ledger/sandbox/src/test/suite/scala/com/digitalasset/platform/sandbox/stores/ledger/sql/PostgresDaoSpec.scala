// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  NodeCreate,
  NodeExercises,
  NodeFetch
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  ValueText,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{
  Contract,
  PersistenceEntry,
  PostgresLedgerDao
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import com.digitalasset.ledger.api.domain.{LedgerId, RejectionReason}

//TODO: use scalacheck when we have generators available for contracts and transactions
@SuppressWarnings(Array("org.wartremover.warts.Any"))
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
    Await.result(ledgerDao.initializeLedger(LedgerId("test-ledger"), 0), Duration.Inf)
  }

  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")
  private val someValueText = ValueText("some text")
  private val agreement = "agreement"

  "Postgres Ledger DAO" should {

    val event1: EventId = "event1"
    val event2: EventId = "event2"

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
        VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
        agreement
      )
      val keyWithMaintainers = KeyWithMaintainers(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key")),
        Set(alice)
      )

      val contract = Contract(
        absCid,
        let,
        "trId1",
        Some("workflowId"),
        Set(alice, bob),
        Map(alice -> "trId1", bob -> "trId1"),
        contractInstance,
        Some(keyWithMaintainers)
      )

      val transaction = LedgerEntry.Transaction(
        Some("commandId1"),
        "trId1",
        Some("appID1"),
        Some("Alice"),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          Map(
            event1 -> NodeCreate(
              absCid,
              contractInstance,
              None,
              Set(alice, bob),
              Set(alice, bob),
              Some(keyWithMaintainers)
            )),
          ImmArray(event1),
          Set.empty
        ),
        Map(event1 -> Set[Party]("Alice", "Bob"), event2 -> Set[Party]("Alice", "In", "Chains"))
      )

      for {
        result1 <- ledgerDao.lookupActiveContract(absCid)
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          PersistenceEntry.Transaction(
            transaction,
            Map.empty,
            Map(
              absCid -> Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob")))
          )
        )
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
        _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, PersistenceEntry.Checkpoint(checkpoint))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(checkpoint)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    val rejectionReasonGen: Gen[RejectionReason] =
      for {
        const <- Gen.oneOf[String => RejectionReason](
          Seq[String => RejectionReason](
            RejectionReason.Inconsistent.apply(_),
            RejectionReason.OutOfQuota.apply(_),
            RejectionReason.TimedOut.apply(_),
            RejectionReason.Disputed.apply(_),
            RejectionReason.DuplicateCommandId.apply(_)
          ))
        // need to use Arbitrary.arbString to get only valid unicode characters
        desc <- Arbitrary.arbitrary[String].map(_.filterNot(_ == 0)).filter(_.nonEmpty)
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
          _ <- ledgerDao.storeLedgerEntry(offset, offset + 1, PersistenceEntry.Rejection(rejection))
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
        VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
        agreement
      )

      val keyWithMaintainers = KeyWithMaintainers(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key2")),
        Set(Ref.Party.assertFromString("Alice"))
      )

      val contract = Contract(
        absCid,
        let,
        "trId2",
        Some("workflowId"),
        Set(alice, bob),
        Map(alice -> "trId2", bob -> "trId2"),
        contractInstance,
        Some(keyWithMaintainers)
      )

      val transaction = LedgerEntry.Transaction(
        Some("commandId2"),
        "trId2",
        Some("appID2"),
        Some("Alice"),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          Map(
            event1 -> NodeCreate(
              absCid,
              contractInstance,
              None,
              Set(alice, bob),
              Set(alice, bob),
              Some(keyWithMaintainers)
            )),
          ImmArray(event1),
          Set.empty
        ),
        Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          PersistenceEntry.Transaction(transaction, Map.empty, Map.empty))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(transaction)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to load contracts within a transaction" in {
      val offset = nextOffset()
      val absCid = AbsoluteContractId(s"cId$offset")
      val let = Instant.now
      val contractInstance = ContractInst(
        Identifier(
          Ref.PackageId.assertFromString("packageId"),
          Ref.QualifiedName(
            Ref.ModuleName.assertFromString("moduleName"),
            Ref.DottedName.assertFromString("name"))),
        VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
        agreement
      )

      val transactionId = s"trId$offset"
      val contract = Contract(
        absCid,
        let,
        transactionId,
        Some("workflowId"),
        Set(alice, bob),
        Map(alice -> transactionId, bob -> transactionId),
        contractInstance,
        None
      )

      val transaction = LedgerEntry.Transaction(
        Some(s"commandId$offset"),
        transactionId,
        Some(s"appID$offset"),
        Some("Alice"),
        Some("workflowId"),
        let,
        // normally the record time is some time after the ledger effective time
        let.plusMillis(42),
        GenTransaction(
          Map(
            event1 -> NodeCreate(
              absCid,
              contractInstance,
              None,
              Set(alice, bob),
              Set(alice, bob),
              None
            ),
            event2 -> NodeFetch(
              absCid,
              contractInstance.template,
              None,
              Some(Set(alice, bob)),
              Set(alice, bob),
              Set(alice, bob)
            )
          ),
          ImmArray(event1, event2),
          Set.empty
        ),
        Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        resp <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          PersistenceEntry.Transaction(transaction, Map.empty, Map.empty))
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
          VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
          agreement
        )
        val contract = Contract(
          absCid,
          let,
          txId,
          Some("workflowId"),
          Set(alice, bob),
          Map(alice -> txId, bob -> txId),
          contractInstance,
          None
        )

        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          txId,
          Some("appID1"),
          Some("Alice"),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            Map(
              (s"event$id": EventId) -> NodeCreate(
                absCid,
                contractInstance,
                None,
                Set(alice, bob),
                Set(alice, bob),
                None
              )),
            ImmArray[EventId](s"event$id"),
            Set.empty
          ),
          Map((s"event$id": EventId) -> Set("Alice", "Bob"))
        )
      }

      def genExerciseTransaction(id: Long, targetCid: AbsoluteContractId) = {
        val txId = s"trId$id"
        val absCid = AbsoluteContractId(s"cId$id")
        val let = Instant.now
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          txId,
          Some("appID1"),
          Some("Alice"),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            Map(
              (s"event$id": EventId) -> NodeExercises(
                targetCid,
                templateId,
                Ref.Name.assertFromString("choice"),
                None,
                true,
                Set(alice),
                VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some choice value")),
                Set(alice, bob),
                Set(alice, bob),
                ImmArray.empty,
                Some(
                  VersionedValue(
                    ValueVersions.acceptedVersions.head,
                    ValueText("some exercise result"))),
              )),
            ImmArray[EventId](s"event$id"),
            Set.empty
          ),
          Map((s"event$id": EventId) -> Set("Alice", "Bob"))
        )
      }

      def storeCreateTransaction() = {
        val offset = nextOffset()
        val t = genCreateTransaction(offset)
        ledgerDao
          .storeLedgerEntry(
            offset,
            offset + 1,
            PersistenceEntry.Transaction(t, Map.empty, Map.empty))
          .map(_ => ())
      }

      def storeExerciseTransaction(targetCid: AbsoluteContractId) = {
        val offset = nextOffset()
        val t = genExerciseTransaction(offset, targetCid)
        ledgerDao
          .storeLedgerEntry(
            offset,
            offset + 1,
            PersistenceEntry.Transaction(t, Map.empty, Map.empty))
          .map(_ => ())
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

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

}

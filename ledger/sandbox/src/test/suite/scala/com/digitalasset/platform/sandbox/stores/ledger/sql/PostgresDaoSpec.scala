// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.index.v2
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarReader
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
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.domain.{LedgerId, RejectionReason}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao._
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.util.{Success, Try}

//TODO: use scalacheck when we have generators available for contracts and transactions
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PostgresDaoSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with PostgresAroundAll
    with OptionValues {

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private[this] var dbDispatcher: DbDispatcher = _
  private[this] var ledgerDao: LedgerDao = _

  private val nextOffset: () => Long = {
    val counter = new AtomicLong(0)
    () =>
      counter.getAndIncrement()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    dbDispatcher = DbDispatcher(postgresFixture.jdbcUrl, JdbcLedgerDao.Postgres, 4, 4)
    ledgerDao = JdbcLedgerDao(
      dbDispatcher,
      ContractSerializer,
      TransactionSerializer,
      ValueSerializer,
      KeyHasher,
      JdbcLedgerDao.Postgres)
    Await.result(ledgerDao.initializeLedger(LedgerId("test-ledger"), 0), 10.seconds)
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
        Some(keyWithMaintainers),
        Set(alice, bob),
        Set.empty
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
          None,
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
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          None,
          PersistenceEntry.Checkpoint(checkpoint))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(checkpoint)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to persist and load a rejection" in {
      val offset = nextOffset()
      val rejection = LedgerEntry.Rejection(
        Instant.now,
        s"commandId-$offset",
        s"applicationId-$offset",
        "party",
        RejectionReason.Inconsistent("\uED7Eᇫ뭳ꝳꍆꃓ왎"))

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          None,
          PersistenceEntry.Rejection(rejection))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(rejection)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "refuse to persist an upload with no packages" in {
      recoverToSucceededIf[IllegalArgumentException] {
        ledgerDao.uploadLfPackages(UUID.randomUUID().toString, Nil)
      }
    }

    "refuse to persist an upload with an empty id" in {
      recoverToSucceededIf[IllegalArgumentException] {
        ledgerDao.uploadLfPackages("", PostgresDaoSpec.Fixtures.packages)
      }
    }

    "upload packages in an idempotent fashion, maintaining existing descriptions" in {
      val firstDescription = "first description"
      val secondDescription = "second description"
      for {
        firstUploadResult <- ledgerDao
          .uploadLfPackages(
            UUID.randomUUID().toString,
            PostgresDaoSpec.Fixtures.packages
              .map(a => a._1 -> a._2.copy(sourceDescription = Some(firstDescription)))
              .take(1))
        secondUploadResult <- ledgerDao
          .uploadLfPackages(
            UUID.randomUUID().toString,
            PostgresDaoSpec.Fixtures.packages.map(a =>
              a._1 -> a._2.copy(sourceDescription = Some(secondDescription))))
        loadedPackages <- ledgerDao.listLfPackages
      } yield {
        firstUploadResult shouldBe Map(PersistenceResponse.Ok -> 1)
        secondUploadResult shouldBe Map(
          PersistenceResponse.Ok -> 2,
          PersistenceResponse.Duplicate -> 1)
        loadedPackages.values.flatMap(_.sourceDescription.toList) should contain theSameElementsAs Seq(
          firstDescription,
          secondDescription,
          secondDescription)
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
          None,
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
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          None,
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
                consuming = true,
                Set(alice),
                VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some choice value")),
                Set(alice, bob),
                Set(alice, bob),
                ImmArray.empty,
                Some(
                  VersionedValue(
                    ValueVersions.acceptedVersions.head,
                    ValueText("some exercise result"))),
                None
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
            None,
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
            None,
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
        startingSnapshotSize <- startingSnapshot.acs.map(_ => 1).runWith(sumSink)
        snapshotSize <- snapshot.acs.map(_ => 1).runWith(sumSink)
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

object PostgresDaoSpec {

  object Fixtures extends BazelRunfiles {

    private val reader = DarReader { (_, stream) =>
      Try(DamlLf.Archive.parseFrom(stream))
    }
    private val Success(dar) =
      reader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar")))
    private val now = Instant.now()

    val packages =
      dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))

  }

}

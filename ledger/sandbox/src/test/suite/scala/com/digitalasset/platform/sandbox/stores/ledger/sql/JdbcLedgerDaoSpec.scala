// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.{Offset, Configuration, TimeModelImpl}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.LedgerString.ordering
import com.digitalasset.daml.lf.data.Ref.{Identifier, LedgerString, Party}
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
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  LedgerId,
  RejectionReason,
  TransactionFilter
}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.{ConfigurationEntry, LedgerEntry}
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao._
import com.digitalasset.platform.sandbox.stores.ledger.sql.migration.FlywayMigrations
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.collection.immutable.TreeMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.util.{Success, Try}

//TODO: use scalacheck when we have generators available for contracts and transactions
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class JdbcLedgerDaoSpec
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
    val loggerFactory = NamedLoggerFactory(JdbcLedgerDaoSpec.getClass)
    FlywayMigrations(postgresFixture.jdbcUrl, loggerFactory).migrate()
    dbDispatcher = DbDispatcher(postgresFixture.jdbcUrl, 4, 4, loggerFactory, new MetricRegistry)
    ledgerDao = JdbcLedgerDao(
      dbDispatcher,
      ContractSerializer,
      TransactionSerializer,
      ValueSerializer,
      KeyHasher,
      DbType.Postgres,
      loggerFactory,
      system.dispatcher)
    Await.result(ledgerDao.initializeLedger(LedgerId("test-ledger"), 0), 10.seconds)
  }

  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")
  private val charlie = Party.assertFromString("Charlie")
  private val someValueText = ValueText("some text")
  private val agreement = "agreement"

  private val nextExternalOffset = {
    val n = new AtomicLong(0)
    () =>
      Some(Offset(Array.fill(3)(n.getAndIncrement())).toLedgerString)
  }

  "JDBC Ledger DAO" should {

    val event1: EventId = "event1"
    val event2: EventId = "event2"

    def persistAndLoadContractsTest(externalOffset: Option[LedgerString]) = {
      val offset = nextOffset()
      val absCid = AbsoluteContractId(s"cId1-$offset")
      val txId = s"trId-$offset"
      val workflowId = s"workflowId-$offset"
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
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText(s"key-$offset")),
        Set(alice)
      )

      val contract = ActiveContract(
        absCid,
        let,
        txId,
        event1,
        Some(workflowId),
        contractInstance,
        Set(alice, bob),
        Map(alice -> txId, bob -> txId),
        Some(keyWithMaintainers),
        Set(alice, bob),
        Set.empty,
        contractInstance.agreementText
      )

      val transaction = LedgerEntry.Transaction(
        Some("commandId1"),
        txId,
        Some(s"appID-$offset"),
        Some("Alice"),
        Some(workflowId),
        let,
        let,
        GenTransaction(
          TreeMap(
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
        result1 <- ledgerDao.lookupActiveOrDivulgedContract(absCid, alice)
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          externalOffset,
          PersistenceEntry.Transaction(
            transaction,
            Map.empty,
            Map(
              absCid -> Set(
                Ref.Party.assertFromString("Alice"),
                Ref.Party.assertFromString("Bob"))),
            List.empty
          )
        )
        result2 <- ledgerDao.lookupActiveOrDivulgedContract(absCid, alice)
        externalLedgerEnd <- ledgerDao.lookupExternalLedgerEnd()
      } yield {
        result1 shouldEqual None
        result2 shouldEqual Some(contract)
        externalLedgerEnd shouldEqual externalOffset
      }
    }

    "be able to persist and load contracts without external offset" in {
      persistAndLoadContractsTest(None)
    }

    "be able to persist and load contracts with external offset" in {
      persistAndLoadContractsTest(nextExternalOffset())
    }

    def persistAndLoadCheckpointTest(externalOffset: Option[LedgerString]) = {
      val checkpoint = LedgerEntry.Checkpoint(Instant.now)
      val offset = nextOffset()

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          externalOffset,
          PersistenceEntry.Checkpoint(checkpoint))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
        externalLedgerEnd <- ledgerDao.lookupExternalLedgerEnd()
      } yield {
        entry shouldEqual Some(checkpoint)
        endingOffset shouldEqual (startingOffset + 1)
        externalLedgerEnd shouldEqual externalOffset
      }
    }

    "be able to persist and load a checkpoint without external offset" in {
      persistAndLoadCheckpointTest(None)
    }

    "be able to persist and load a checkpoint with external offset" in {
      persistAndLoadCheckpointTest(nextExternalOffset())
    }

    def persistAndLoadRejection(externalOffset: Option[LedgerString]) = {
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
          externalOffset,
          PersistenceEntry.Rejection(rejection))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
        externalLedgerEnd <- ledgerDao.lookupExternalLedgerEnd()
      } yield {
        entry shouldEqual Some(rejection)
        endingOffset shouldEqual (startingOffset + 1)
        externalLedgerEnd shouldEqual externalOffset
      }

    }
    "be able to persist and load a rejection without external offset" in {
      persistAndLoadRejection(None)
    }

    "be able to persist and load a rejection with external offset" in {
      persistAndLoadRejection(nextExternalOffset())
    }

    val defaultConfig = Configuration(
      generation = 0,
      timeModel = TimeModelImpl.reasonableDefault,
      authorizedParticipantIds = List.empty)

    "be able to persist and load configuration" in {
      val offset = nextOffset()
      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        startingConfig <- ledgerDao.lookupLedgerConfiguration()

        response <- ledgerDao.storeConfigurationEntry(
          offset,
          offset + 1,
          None,
          Instant.EPOCH,
          "submission-0",
          Ref.LedgerString.assertFromString("participant-0"),
          defaultConfig,
          None
        )
        storedConfig <- ledgerDao.lookupLedgerConfiguration()
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        response shouldEqual PersistenceResponse.Ok
        startingConfig shouldEqual None
        storedConfig shouldEqual Some(defaultConfig)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to persist configuration rejection" in {
      val offset = nextOffset()
      val participantId = Ref.LedgerString.assertFromString("participant-0")
      for {
        startingConfig <- ledgerDao.lookupLedgerConfiguration()
        proposedConfig = startingConfig.getOrElse(defaultConfig)
        response <- ledgerDao.storeConfigurationEntry(
          offset,
          offset + 1,
          None,
          Instant.EPOCH,
          "config-rejection-0",
          participantId,
          proposedConfig,
          Some("bad config")
        )
        storedConfig <- ledgerDao.lookupLedgerConfiguration()
        entries <- ledgerDao.getConfigurationEntries(offset, offset + 1).runWith(Sink.seq)

      } yield {
        response shouldEqual PersistenceResponse.Ok
        startingConfig shouldEqual storedConfig
        entries shouldEqual List(
          offset -> ConfigurationEntry
            .Rejected("config-rejection-0", participantId, "bad config", proposedConfig)
        )
      }
    }

    "refuse to persist invalid configuration entry" in {
      val offset0 = nextOffset()
      val participantId = Ref.LedgerString.assertFromString("participant-0")
      for {
        config <- ledgerDao.lookupLedgerConfiguration().map(_.getOrElse(defaultConfig))

        // Store a new configuration with a known submission id
        resp0 <- ledgerDao.storeConfigurationEntry(
          offset0,
          offset0 + 1,
          None,
          Instant.EPOCH,
          "refuse-config-0",
          participantId,
          config.copy(generation = config.generation + 1),
          None
        )
        newConfig <- ledgerDao.lookupLedgerConfiguration().map(_.get)

        // Submission with duplicate submissionId is rejected
        offset1 = nextOffset()
        resp1 <- ledgerDao.storeConfigurationEntry(
          offset1,
          offset1 + 1,
          None,
          Instant.EPOCH,
          "refuse-config-0",
          participantId,
          newConfig.copy(generation = config.generation + 1),
          None
        )

        // Submission with mismatching generation is rejected
        offset2 = nextOffset()
        resp2 <- ledgerDao.storeConfigurationEntry(
          offset2,
          offset2 + 1,
          None,
          Instant.EPOCH,
          "refuse-config-1",
          participantId,
          config,
          None
        )

        // Submission with unique submissionId and correct generation is accepted.
        offset3 = nextOffset()
        lastConfig = newConfig.copy(generation = newConfig.generation + 1)
        resp3 <- ledgerDao.storeConfigurationEntry(
          offset3,
          offset3 + 1,
          None,
          Instant.EPOCH,
          "refuse-config-2",
          participantId,
          lastConfig,
          None
        )
        lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.get)

        entries <- ledgerDao.getConfigurationEntries(offset0, offset3 + 1).runWith(Sink.seq)
      } yield {
        resp0 shouldEqual PersistenceResponse.Ok
        resp1 shouldEqual PersistenceResponse.Duplicate
        resp2 shouldEqual PersistenceResponse.Duplicate
        resp3 shouldEqual PersistenceResponse.Ok
        lastConfig shouldEqual lastConfigActual
        entries shouldEqual List(
          offset0 -> ConfigurationEntry.Accepted("refuse-config-0", participantId, newConfig),
          offset3 -> ConfigurationEntry.Accepted("refuse-config-2", participantId, lastConfig)
        )
      }
    }

    "refuse to persist an upload with no packages without external offset" in {
      recoverToSucceededIf[IllegalArgumentException] {
        ledgerDao.uploadLfPackages(UUID.randomUUID().toString, Nil, None)
      }
    }

    "refuse to persist an upload with no packages with external offset" in {
      for {
        beforeExternalLedgerEnd <- ledgerDao.lookupExternalLedgerEnd()
        _ <- recoverToSucceededIf[IllegalArgumentException] {
          ledgerDao.uploadLfPackages(UUID.randomUUID().toString, Nil, nextExternalOffset())
        }
        afterExternalLedgerEnd <- ledgerDao.lookupExternalLedgerEnd()

      } yield beforeExternalLedgerEnd shouldEqual afterExternalLedgerEnd
    }

    "refuse to persist an upload with an empty id" in {
      recoverToSucceededIf[IllegalArgumentException] {
        ledgerDao.uploadLfPackages("", JdbcLedgerDaoSpec.Fixtures.packages, None)
      }
    }

    "upload packages in an idempotent fashion, maintaining existing descriptions" in {
      val firstDescription = "first description"
      val secondDescription = "second description"
      for {
        firstUploadResult <- ledgerDao
          .uploadLfPackages(
            UUID.randomUUID().toString,
            JdbcLedgerDaoSpec.Fixtures.packages
              .map(a => a._1 -> a._2.copy(sourceDescription = Some(firstDescription)))
              .take(1),
            None)
        secondUploadResult <- ledgerDao
          .uploadLfPackages(
            UUID.randomUUID().toString,
            JdbcLedgerDaoSpec.Fixtures.packages.map(a =>
              a._1 -> a._2.copy(sourceDescription = Some(secondDescription))),
            None)
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
          TreeMap(
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
          PersistenceEntry.Transaction(transaction, Map.empty, Map.empty, List.empty))
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
          TreeMap(
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
          PersistenceEntry.Transaction(transaction, Map.empty, Map.empty, List.empty))
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
            TreeMap(
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
            TreeMap(
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
            PersistenceEntry.Transaction(t, Map.empty, Map.empty, List.empty))
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
            PersistenceEntry.Transaction(t, Map.empty, Map.empty, List.empty))
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
      val aliceWildcardFilter =
        EventFilter.byTemplates(TransactionFilter(Map(alice -> Filters(None))))
      val aliceSpecificTemplatesFilter = EventFilter.byTemplates(
        TransactionFilter(Map(alice -> Filters(InclusiveFilters(Set(templateId))))))

      val charlieWildcardFilter =
        EventFilter.byTemplates(TransactionFilter(Map(charlie -> Filters(None))))
      val charlieSpecificFilter = EventFilter.byTemplates(
        TransactionFilter(Map(charlie -> Filters(InclusiveFilters(Set(templateId))))))

      val mixedFilter = EventFilter.byTemplates(
        TransactionFilter(
          Map(
            alice -> Filters(InclusiveFilters(Set(templateId))),
            bob -> Filters(None),
            charlie -> Filters(None),
          )))

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        aliceStartingSnapshot <- ledgerDao.getActiveContractSnapshot(
          startingOffset,
          aliceWildcardFilter)
        charlieStartingSnapshot <- ledgerDao.getActiveContractSnapshot(
          startingOffset,
          charlieWildcardFilter)

        mixedStartingSnapshot <- ledgerDao.getActiveContractSnapshot(startingOffset, mixedFilter)

        _ <- runSequentially(N, _ => storeCreateTransaction())
        _ <- storeExerciseTransaction(AbsoluteContractId(s"cId$startingOffset"))

        snapshotOffset <- ledgerDao.lookupLedgerEnd()

        aliceWildcardSnapshot <- ledgerDao.getActiveContractSnapshot(
          snapshotOffset,
          aliceWildcardFilter)
        aliceSpecificTemplatesSnapshot <- ledgerDao.getActiveContractSnapshot(
          snapshotOffset,
          aliceSpecificTemplatesFilter)

        charlieWildcardSnapshot <- ledgerDao.getActiveContractSnapshot(
          snapshotOffset,
          charlieWildcardFilter)
        charlieSpecificTemplateSnapshot <- ledgerDao.getActiveContractSnapshot(
          snapshotOffset,
          charlieSpecificFilter)

        mixedSnapshot <- ledgerDao.getActiveContractSnapshot(snapshotOffset, mixedFilter)

        _ <- runSequentially(M, _ => storeCreateTransaction())

        endingOffset <- ledgerDao.lookupLedgerEnd()

        aliceStartingSnapshotSize <- aliceStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
        aliceWildcardSnapshotSize <- aliceWildcardSnapshot.acs.map(_ => 1).runWith(sumSink)
        aliceSpecificTemplatesSnapshotSize <- aliceSpecificTemplatesSnapshot.acs
          .map(_ => 1)
          .runWith(sumSink)

        charlieStartingSnapshotSize <- charlieStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
        charlieWildcardSnapshotSize <- charlieWildcardSnapshot.acs.map(_ => 1).runWith(sumSink)
        charlieSpecificTemplateSnapshotSize <- charlieSpecificTemplateSnapshot.acs
          .map(_ => 1)
          .runWith(sumSink)

        mixedStartingSnapshotSize <- mixedStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
        mixedSnapshotSize <- mixedSnapshot.acs.map(_ => 1).runWith(sumSink)

      } yield {
        withClue("starting offset: ") {
          aliceStartingSnapshot.offset shouldEqual startingOffset
        }
        withClue("snapshot offset: ") {
          aliceWildcardSnapshot.offset shouldEqual snapshotOffset
          aliceSpecificTemplatesSnapshot.offset shouldEqual snapshotOffset
        }
        withClue("snapshot offset (2): ") {
          snapshotOffset shouldEqual (startingOffset + N + 1)
        }
        withClue("ending offset: ") {
          endingOffset shouldEqual (snapshotOffset + M)
        }
        withClue("alice wildcard snapshot size: ") {
          (aliceWildcardSnapshotSize - aliceStartingSnapshotSize) shouldEqual (N - 1)
        }
        withClue("alice specific template snapshot size: ") {
          (aliceSpecificTemplatesSnapshotSize - aliceStartingSnapshotSize) shouldEqual (N - 1)
        }
        withClue("charlie wildcard snapshot size: ") {
          (charlieWildcardSnapshotSize - charlieStartingSnapshotSize) shouldEqual 0
        }
        withClue("charlie specific template snapshot size: ") {
          (charlieSpecificTemplateSnapshotSize - charlieStartingSnapshotSize) shouldEqual 0
        }
        withClue("mixed snapshot size: ") {
          (mixedSnapshotSize - mixedStartingSnapshotSize) shouldEqual (N - 1)
        }
      }
    }

  }

  private implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  private implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

}

object JdbcLedgerDaoSpec {

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

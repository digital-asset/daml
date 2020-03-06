// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.{Configuration, Offset, TimeModel}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.{Identifier, LedgerString, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  ValueRecord,
  ValueText,
  ValueUnit,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  LedgerId,
  PartyDetails,
  RejectionReason,
  TransactionFilter
}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.store.entries.{ConfigurationEntry, LedgerEntry, PartyLedgerEntry}
import com.digitalasset.platform.store.{DbType, FlywayMigrations, PersistenceEntry}
import com.digitalasset.resources.Resource
import com.digitalasset.testing.postgresql.PostgresAroundAll
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.collection.immutable.HashMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
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
  private[this] var resource: Resource[LedgerDao] = _
  private[this] var ledgerDao: LedgerDao = _

  private val nextOffset: () => Long = {
    val counter = new AtomicLong(0)
    () =>
      counter.getAndIncrement()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    implicit val executionContext: ExecutionContext = system.dispatcher
    resource = newLoggingContext { implicit logCtx =>
      for {
        _ <- Resource.fromFuture(new FlywayMigrations(postgresFixture.jdbcUrl).migrate())
        dbDispatcher <- DbDispatcher
          .owner(postgresFixture.jdbcUrl, 4, new MetricRegistry)
          .acquire()
        ledgerDao = JdbcLedgerDao(dbDispatcher, DbType.Postgres, executionContext)
        _ <- Resource.fromFuture(ledgerDao.initializeLedger(LedgerId("test-ledger"), 0))
      } yield ledgerDao
    }
    ledgerDao = Await.result(resource.asFuture, 10.seconds)
  }

  override def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }

  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")
  private val charlie = Party.assertFromString("Charlie")

  private val someAgreement = "agreement"
  private val someTemplateId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someTemplate"))
  )
  private val someRecordId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someRecord"))
  )
  private val someValueText = ValueText("some text")
  private val someValueRecord = ValueRecord(
    Some(someRecordId),
    ImmArray(Some(Ref.Name.assertFromString("field")) -> someValueText))
  private val someContractKey = VersionedValue(ValueVersions.acceptedVersions.head, someValueText)
  private val someContractInstance = ContractInst(
    someTemplateId,
    VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
    someAgreement
  )

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
      val keyWithMaintainers = KeyWithMaintainers(
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText(s"key-$offset")),
        Set(alice)
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
          HashMap(
            event1 -> NodeCreate(
              nodeSeed = None,
              coid = absCid,
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(alice, bob),
              stakeholders = Set(alice, bob),
              key = Some(keyWithMaintainers)
            )),
          ImmArray(event1),
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
        result2 shouldEqual Some(someContractInstance)
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
      timeModel = TimeModel.reasonableDefault
    )

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
          s"submission-$offset",
          Ref.ParticipantId.assertFromString("participant-0"),
          defaultConfig,
          None
        )
        optStoredConfig <- ledgerDao.lookupLedgerConfiguration()
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        response shouldEqual PersistenceResponse.Ok
        startingConfig shouldEqual None
        optStoredConfig.map(_._2) shouldEqual Some(defaultConfig)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to persist configuration rejection" in {
      val offset = nextOffset()
      val participantId = Ref.ParticipantId.assertFromString("participant-0")
      for {
        startingConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2))
        proposedConfig = startingConfig.getOrElse(defaultConfig)
        response <- ledgerDao.storeConfigurationEntry(
          offset,
          offset + 1,
          None,
          Instant.EPOCH,
          s"config-rejection-$offset",
          participantId,
          proposedConfig,
          Some("bad config")
        )
        storedConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2))
        entries <- ledgerDao.getConfigurationEntries(offset, offset + 1).runWith(Sink.seq)

      } yield {
        response shouldEqual PersistenceResponse.Ok
        startingConfig shouldEqual storedConfig
        entries shouldEqual List(
          offset -> ConfigurationEntry
            .Rejected(s"config-rejection-$offset", participantId, "bad config", proposedConfig)
        )
      }
    }

    "refuse to persist invalid configuration entry" in {
      val offset0 = nextOffset()
      val participantId = Ref.ParticipantId.assertFromString("participant-0")
      for {
        config <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).getOrElse(defaultConfig))

        // Store a new configuration with a known submission id
        submissionId = s"refuse-config-$offset0"
        resp0 <- ledgerDao.storeConfigurationEntry(
          offset0,
          offset0 + 1,
          None,
          Instant.EPOCH,
          submissionId,
          participantId,
          config.copy(generation = config.generation + 1),
          None
        )
        newConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

        // Submission with duplicate submissionId is rejected
        offset1 = nextOffset()
        resp1 <- ledgerDao.storeConfigurationEntry(
          offset1,
          offset1 + 1,
          None,
          Instant.EPOCH,
          submissionId,
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
          s"refuse-config-$offset2",
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
          s"refuse-config-$offset3",
          participantId,
          lastConfig,
          None
        )
        lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

        entries <- ledgerDao.getConfigurationEntries(offset0, offset3 + 1).runWith(Sink.seq)
      } yield {
        resp0 shouldEqual PersistenceResponse.Ok
        resp1 shouldEqual PersistenceResponse.Duplicate
        resp2 shouldEqual PersistenceResponse.Ok
        resp3 shouldEqual PersistenceResponse.Ok
        lastConfig shouldEqual lastConfigActual
        entries.toList shouldEqual List(
          offset0 -> ConfigurationEntry
            .Accepted(s"refuse-config-$offset0", participantId, newConfig),
          /* offset1 is duplicate */
          offset2 -> ConfigurationEntry
            .Rejected(
              s"refuse-config-$offset2",
              participantId,
              "Generation mismatch: expected=2, actual=0",
              config),
          offset3 -> ConfigurationEntry
            .Accepted(s"refuse-config-$offset3", participantId, lastConfig)
        )
      }
    }

    "store and retrieve all parties" in {
      val alice = PartyDetails(
        party = Ref.Party.assertFromString(s"Alice-${UUID.randomUUID()}"),
        displayName = Some("Alice Arkwright"),
        isLocal = true,
      )
      val bob = PartyDetails(
        party = Ref.Party.assertFromString(s"Bob-${UUID.randomUUID()}"),
        displayName = Some("Bob Bobertson"),
        isLocal = true,
      )
      val participantId = Ref.ParticipantId.assertFromString("participant-0")
      val offset1 = nextOffset()
      for {
        response <- ledgerDao.storePartyEntry(
          offset1,
          offset1 + 1,
          None,
          PartyLedgerEntry.AllocationAccepted(
            submissionIdOpt = Some(UUID.randomUUID().toString),
            participantId = participantId,
            recordTime = Instant.now,
            partyDetails = alice,
          ),
        )
        _ = response should be(PersistenceResponse.Ok)
        offset2 = nextOffset()
        response <- ledgerDao.storePartyEntry(
          offset2,
          offset2 + 1,
          None,
          PartyLedgerEntry.AllocationAccepted(
            submissionIdOpt = Some(UUID.randomUUID().toString),
            participantId = participantId,
            recordTime = Instant.now,
            partyDetails = bob,
          ),
        )
        _ = response should be(PersistenceResponse.Ok)
        parties <- ledgerDao.getParties
      } yield {
        parties should contain allOf (alice, bob)
      }
    }

    "upload packages in an idempotent fashion, maintaining existing descriptions" in {
      val firstDescription = "first description"
      val secondDescription = "second description"
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      for {
        firstUploadResult <- ledgerDao
          .storePackageEntry(
            offset1,
            offset1 + 1,
            None,
            JdbcLedgerDaoSpec.Fixtures.packages
              .map(a => a._1 -> a._2.copy(sourceDescription = Some(firstDescription)))
              .take(1),
            None)
        secondUploadResult <- ledgerDao
          .storePackageEntry(
            offset2,
            offset2 + 1,
            None,
            JdbcLedgerDaoSpec.Fixtures.packages.map(a =>
              a._1 -> a._2.copy(sourceDescription = Some(secondDescription))),
            None)
        loadedPackages <- ledgerDao.listLfPackages
      } yield {
        firstUploadResult shouldBe PersistenceResponse.Ok
        secondUploadResult shouldBe PersistenceResponse.Ok
        // Note that the order here isn’t fixed.
        loadedPackages.values.flatMap(_.sourceDescription.toList) should contain theSameElementsAs
          Seq(firstDescription) ++ Seq.fill(JdbcLedgerDaoSpec.Fixtures.packages.length - 1)(
            secondDescription)
      }
    }

    "be able to persist and load a transaction" in {
      val offset = nextOffset()
      val absCid = AbsoluteContractId("cId2")
      val let = Instant.now

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
          HashMap(
            event1 -> NodeCreate(
              nodeSeed = None,
              coid = absCid,
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(alice, bob),
              stakeholders = Set(alice, bob),
              key = Some(keyWithMaintainers)
            )),
          ImmArray(event1),
        ),
        Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          None,
          PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
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
          HashMap(
            event1 -> NodeCreate(
              nodeSeed = None,
              coid = absCid,
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(alice, bob),
              stakeholders = Set(alice, bob),
              key = None
            ),
            event2 -> NodeFetch(
              absCid,
              someContractInstance.template,
              None,
              Some(Set(alice, bob)),
              Set(alice, bob),
              Set(alice, bob)
            )
          ),
          ImmArray(event1, event2),
        ),
        Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(
          offset,
          offset + 1,
          None,
          PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(transaction)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    "be able to produce a valid snapshot" in {
      def genCreateTransaction(id: Long) = {
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
            HashMap(
              (s"event$id": EventId) -> NodeCreate(
                nodeSeed = None,
                coid = absCid,
                coinst = someContractInstance,
                optLocation = None,
                signatories = Set(alice, bob),
                stakeholders = Set(alice, bob),
                key = None
              )),
            ImmArray[EventId](s"event$id"),
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
            HashMap(
              (s"event$id": EventId) -> NodeExercises(
                nodeSeed = None,
                targetCoid = targetCid,
                templateId = someTemplateId,
                choiceId = Ref.Name.assertFromString("choice"),
                optLocation = None,
                consuming = true,
                actingParties = Set(alice),
                chosenValue = VersionedValue(
                  ValueVersions.acceptedVersions.head,
                  ValueText("some choice value")),
                stakeholders = Set(alice, bob),
                signatories = Set(alice, bob),
                children = ImmArray.empty,
                exerciseResult = Some(
                  VersionedValue(
                    ValueVersions.acceptedVersions.head,
                    ValueText("some exercise result"))),
                key = None
              )),
            ImmArray[EventId](s"event$id"),
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
            PersistenceEntry.Transaction(t, Map.empty, List.empty))
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
            PersistenceEntry.Transaction(t, Map.empty, List.empty))
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
        TransactionFilter(Map(alice -> Filters(None)))
      val aliceSpecificTemplatesFilter =
        TransactionFilter(Map(alice -> Filters(InclusiveFilters(Set(someTemplateId)))))

      val charlieWildcardFilter =
        TransactionFilter(Map(charlie -> Filters(None)))
      val charlieSpecificFilter =
        TransactionFilter(Map(charlie -> Filters(InclusiveFilters(Set(someTemplateId)))))

      val mixedFilter =
        TransactionFilter(
          Map(
            alice -> Filters(InclusiveFilters(Set(someTemplateId))),
            bob -> Filters(None),
            charlie -> Filters(None),
          ))

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

    /** A transaction that creates the given key */
    def txCreateContractWithKey(let: Instant, id: Long, party: Party, key: String) =
      PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(party),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            HashMap(
              (s"event$id": EventId) -> NodeCreate(
                nodeSeed = None,
                coid = AbsoluteContractId(s"contractId$id"),
                coinst = someContractInstance,
                optLocation = None,
                signatories = Set(party),
                stakeholders = Set(party),
                key = Some(
                  KeyWithMaintainers(
                    VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                    Set(party)))
              )),
            ImmArray[EventId](s"event$id"),
          ),
          Map((s"event$id": EventId) -> Set(party))
        ),
        Map.empty,
        List.empty
      )

    /** A transaction that archives the given contract with the given key */
    def txArchiveContract(let: Instant, id: Long, party: Party, cid: Long, key: String) =
      PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(party),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            HashMap(
              (s"event$id": EventId) -> NodeExercises(
                nodeSeed = None,
                targetCoid = AbsoluteContractId(s"contractId$cid"),
                templateId = someTemplateId,
                choiceId = Ref.ChoiceName.assertFromString("Archive"),
                optLocation = None,
                consuming = true,
                actingParties = Set(party),
                chosenValue = VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit),
                stakeholders = Set(party),
                signatories = Set(party),
                controllers = Set(party),
                children = ImmArray.empty,
                exerciseResult =
                  Some(VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit)),
                key = Some(
                  KeyWithMaintainers(
                    VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                    Set(party)))
              )),
            ImmArray[EventId](s"event$id"),
          ),
          Map((s"event$id": EventId) -> Set(party))
        ),
        Map.empty,
        List.empty
      )

    /** A transaction that looks up a key */
    def txLookupByKey(let: Instant, id: Long, party: Party, key: String, result: Option[Long]) =
      PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(party),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            HashMap(
              (s"event$id": EventId) -> NodeLookupByKey(
                someTemplateId,
                None,
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                  Set(party)),
                result.map(id => AbsoluteContractId(s"contractId$id")),
              )),
            ImmArray[EventId](s"event$id"),
          ),
          Map((s"event$id": EventId) -> Set(party))
        ),
        Map.empty,
        List.empty
      )

    /** A transaction that fetches a contract Id */
    def txFetch(let: Instant, id: Long, party: Party, cid: Long) =
      PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(party),
          Some("workflowId"),
          let,
          let,
          GenTransaction(
            HashMap(
              (s"event$id": EventId) -> NodeFetch(
                coid = AbsoluteContractId(s"contractId$cid"),
                templateId = someTemplateId,
                optLocation = None,
                actingParties = Some(Set(party)),
                signatories = Set(party),
                stakeholders = Set(party),
              )),
            ImmArray[EventId](s"event$id"),
          ),
          Map((s"event$id": EventId) -> Set(party))
        ),
        Map.empty,
        List.empty
      )

    "refuse to serialize duplicate contract keys" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val keyValue = s"key-$offset1"

      // Scenario: Two concurrent commands create the same contract key.
      // At command interpretation time, the keys do not exist yet.
      // At serialization time, the ledger should refuse to serialize one of them.
      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txCreateContractWithKey(let, offset2, alice, keyValue)
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
        res2 shouldBe a[LedgerEntry.Rejection]
      }
    }

    "serialize a valid positive lookupByKey" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val keyValue = s"key-$offset1"

      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txLookupByKey(let, offset2, alice, keyValue, Some(offset1))
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
      }
    }

    "refuse to serialize invalid negative lookupByKey" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val keyValue = s"key-$offset1"

      // Scenario: Two concurrent commands: one create and one lookupByKey.
      // At command interpretation time, the lookupByKey does not find any contract.
      // At serialization time, it should be rejected because now the key is there.
      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txLookupByKey(let, offset2, alice, keyValue, None)
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
        res2 shouldBe a[LedgerEntry.Rejection]
      }
    }

    "refuse to serialize invalid positive lookupByKey" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val offset3 = nextOffset()
      val keyValue = s"key-$offset1"

      // Scenario: Two concurrent commands: one exercise and one lookupByKey.
      // At command interpretation time, the lookupByKey finds a contract.
      // At serialization time, it should be rejected because now the contract was archived.
      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txArchiveContract(let, offset2, alice, offset1, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset3,
          offset3 + 1,
          None,
          txLookupByKey(let, offset3, alice, keyValue, Some(offset1))
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
        res3 <- ledgerDao.lookupLedgerEntryAssert(offset3)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
        res2 shouldBe a[LedgerEntry.Transaction]
        res3 shouldBe a[LedgerEntry.Rejection]
      }
    }

    "serialize a valid fetch" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val keyValue = s"key-$offset1"

      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txFetch(let, offset2, alice, offset1)
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
        res2 shouldBe a[LedgerEntry.Transaction]
      }
    }

    "refuse to serialize invalid fetch" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val offset3 = nextOffset()
      val keyValue = s"key-$offset1"

      // Scenario: Two concurrent commands: one exercise and one fetch.
      // At command interpretation time, the fetch finds a contract.
      // At serialization time, it should be rejected because now the contract was archived.
      for {
        _ <- ledgerDao.storeLedgerEntry(
          offset1,
          offset1 + 1,
          None,
          txCreateContractWithKey(let, offset1, alice, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          txArchiveContract(let, offset2, alice, offset1, keyValue)
        )
        _ <- ledgerDao.storeLedgerEntry(
          offset3,
          offset3 + 1,
          None,
          txFetch(let, offset3, alice, offset1)
        )
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
        res3 <- ledgerDao.lookupLedgerEntryAssert(offset3)
      } yield {
        res1 shouldBe a[LedgerEntry.Transaction]
        res2 shouldBe a[LedgerEntry.Transaction]
        res3 shouldBe a[LedgerEntry.Rejection]
      }
    }

    "be able to use divulged contract in later transaction" in {
      val let = Instant.now
      val offset1 = nextOffset()
      val offset2 = nextOffset()
      val offset3 = nextOffset()
      def emptyTxWithDivulgedContracts(id: Long) = PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(alice),
          Some("workflowId"),
          let,
          let,
          GenTransaction(HashMap.empty, ImmArray.empty),
          Map.empty
        ),
        Map(AbsoluteContractId(s"contractId$id") -> Set(bob)),
        List(AbsoluteContractId(s"contractId$id") -> someContractInstance)
      )

      for {
        // First try and index a transaction fetching a completely unknown contract.
        _ <- ledgerDao.storeLedgerEntry(offset1, offset1 + 1, None, txFetch(let, offset1, bob, 0))
        res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)

        // Then index a transaction that just divulges the contract to bob.
        _ <- ledgerDao.storeLedgerEntry(
          offset2,
          offset2 + 1,
          None,
          emptyTxWithDivulgedContracts(offset2))
        res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)

        // Finally try and fetch the divulged contract. LedgerDao should be able to look up the divulged contract
        // and index the transaction without finding the contract metadata (LET) for it, as long as the contract
        // exists in contract_data.
        _ <- ledgerDao.storeLedgerEntry(
          offset3,
          offset3 + 1,
          None,
          txFetch(let.plusSeconds(1), offset3, bob, offset2))
        res3 <- ledgerDao.lookupLedgerEntryAssert(offset3)
      } yield {
        res1 shouldBe a[LedgerEntry.Rejection]
        res2 shouldBe a[LedgerEntry.Transaction]
        res3 shouldBe a[LedgerEntry.Transaction]
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

    private[JdbcLedgerDaoSpec] val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
      dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))

  }

}

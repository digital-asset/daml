// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import akka.stream.scaladsl.Sink
import ch.qos.logback.classic.Level
import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.health.Healthy
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.v2.{SubmissionResult, SubmitterInfo, TransactionMeta}
import com.daml.ledger.resources.{Resource, ResourceContext, TestResourceContext}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.LegacyTransactionCommitter
import com.daml.lf.transaction.test.TransactionBuilder.EmptySubmitted
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.SeedService
import com.daml.platform.common.{LedgerIdMode, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.MetricsAround
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedgerSpec._
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{IndexMetadata, LfValueTranslationCache}
import com.daml.platform.testing.LogCollector
import com.daml.testing.postgresql.PostgresAroundEach
import com.daml.timer.RetryStrategy
import io.grpc.Status
import org.mockito.MockitoSugar
import org.scalatest.Inside
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

final class SqlLedgerSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AsyncTimeLimitedTests
    with ScaledTimeSpans
    with Eventually
    with TestResourceContext
    with AkkaBeforeAndAfterAll
    with PostgresAroundEach
    with MetricsAround
    with MockitoSugar {

  import TestIdentifiers._

  protected implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  override val timeLimit: Span = scaled(Span(1, Minute))
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)))

  private val createdLedgers = mutable.Buffer[Resource[Ledger]]()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    createdLedgers.clear()
  }

  override protected def afterEach(): Unit = {
    LogCollector.clear[SqlLedgerSpec]
    for (ledger <- createdLedgers)
      Await.result(ledger.release(), 2.seconds)
    super.afterEach()
  }

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger ID" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        ledger.ledgerId should not be ""
      }
    }

    "be able to be created from scratch with a given ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId)
      } yield {
        ledger.ledgerId should be(ledgerId)
      }
    }

    "be able to be reused keeping the old ledger ID" in {
      for {
        ledger1 <- createSqlLedger(ledgerId)
        ledger2 <- createSqlLedger(ledgerId)
        ledger3 <- createSqlLedger()
      } yield {
        ledger1.ledgerId should not be LedgerId
        ledger1.ledgerId should be(ledger2.ledgerId)
        ledger2.ledgerId should be(ledger3.ledgerId)
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger ID" in {
      for {
        _ <- createSqlLedger(ledgerId)
        throwable <- createSqlLedger(ledgerId = "AnotherLedger").failed
      } yield {
        throwable.getMessage should be(
          "The provided ledger id does not match the existing one. Existing: \"TheLedger\", Provided: \"AnotherLedger\"."
        )
      }
    }

    "correctly initialized the participant ID" in {
      val participantId = makeParticipantId("TheOnlyParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId)
        metadata <- IndexMetadata.read(postgresDatabase.url, mock[ErrorFactories])
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "allow to resume on an existing participant ID" in {
      val participantId = makeParticipantId("TheParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId)
        _ <- createSqlLedgerWithParticipantId(participantId)
        metadata <- IndexMetadata.read(postgresDatabase.url, mock[ErrorFactories])
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "refuse to create a new ledger when there is already one with a different participant ID" in {
      val expectedExisting = makeParticipantId("TheParticipant")
      val expectedProvided = makeParticipantId("AnotherParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(expectedExisting)
        throwable <- createSqlLedgerWithParticipantId(expectedProvided).failed
      } yield {
        throwable match {
          case mismatch: MismatchException.ParticipantId =>
            mismatch.existing shouldEqual expectedExisting
            mismatch.provided shouldEqual expectedProvided
          case _ =>
            fail("Did not get the expected exception type", throwable)
        }
      }
    }

    "load no packages by default" in {
      for {
        ledger <- createSqlLedger()
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "load packages if provided with a dynamic ledger ID" in {
      for {
        ledger <- createSqlLedger(packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load packages if provided with a static ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId = "TheLedger", packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load no packages if the ledger already exists" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger")
        ledger <- createSqlLedger(ledgerId = "TheLedger", packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "be healthy" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        ledger.currentHealth() should be(Healthy)
      }
    }

    "not allow insertion of subsequent parties with the same identifier: operations should succeed without effect" in {
      for {
        sqlLedger <- createSqlLedger()
        partyAllocationResult1 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId1,
          party = party1,
          displayName = Some("displayname1"),
        )
        // This will fail until the party is available
        retrievedParty1 <- eventually(sqlLedger.getParties(Seq(party1)).map(_.head))
        partyAllocationResult2 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId2,
          party = party1,
          displayName = Some("displayname2"),
        )
        partyAllocationResult3 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId3,
          party = party2,
          displayName = None,
        )
        // This will fail until the party which is added the last time is visible
        // This also means that the second party addition is already processed as well
        _ <- eventually(sqlLedger.getParties(Seq(party2)).map(_.head))
        retrievedParty2 <- eventually(sqlLedger.getParties(Seq(party1)).map(_.head))
      } yield {
        partyAllocationResult1 shouldBe SubmissionResult.Acknowledged
        retrievedParty1.displayName shouldBe Some("displayname1")
        partyAllocationResult2 shouldBe SubmissionResult.Acknowledged
        partyAllocationResult3 shouldBe SubmissionResult.Acknowledged
        retrievedParty2.displayName shouldBe Some("displayname1")
        val logs =
          LogCollector.read[this.type]("com.daml.platform.sandbox.stores.ledger.sql.SqlLedger")
        logs should contain(
          Level.WARN -> "Ignoring duplicate party submission with ID party1 for submissionId Some(submissionId2)"
        )
      }
    }

    "publish a transaction" in {
      val now = Time.Timestamp.now()
      for {
        sqlLedger <- createSqlLedger()
        start = sqlLedger.ledgerEnd()
        _ <- sqlLedger.publishConfiguration(
          maxRecordTime = now.add(Duration.ofMinutes(1)),
          submissionId = "configuration",
          config = Configuration.reasonableInitialConfiguration,
        )
        result <- sqlLedger.publishTransaction(
          submitterInfo = SubmitterInfo(
            actAs = List(party1),
            applicationId = applicationId,
            commandId = commandId1,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = None,
            ledgerConfiguration = Configuration.reasonableInitialConfiguration,
          ),
          transactionMeta = emptyTransactionMeta(seedService, ledgerEffectiveTime = now),
          transaction = EmptySubmitted,
        )
        completion <- sqlLedger
          .completions(
            startExclusive = Some(start),
            endInclusive = None,
            applicationId = domain.ApplicationId(applicationId),
            parties = Set(party1),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId1`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.OK.value)
        }
      }
    }

    "reject a transaction if no configuration is found" in {
      val now = Time.Timestamp.now()
      val submissionId = "12345678"
      for {
        sqlLedger <- createSqlLedger()
        start = sqlLedger.ledgerEnd()
        result <- sqlLedger.publishTransaction(
          submitterInfo = SubmitterInfo(
            actAs = List(party1),
            applicationId = applicationId,
            commandId = commandId1,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
            ledgerConfiguration = Configuration.reasonableInitialConfiguration,
          ),
          transactionMeta = emptyTransactionMeta(seedService, ledgerEffectiveTime = now),
          transaction = EmptySubmitted,
        )
        completion <- sqlLedger
          .completions(
            startExclusive = Some(start),
            endInclusive = None,
            applicationId = domain.ApplicationId(applicationId),
            parties = Set(party1),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId1`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.NOT_FOUND.value)
            status.message should be(
              s"LEDGER_CONFIGURATION_NOT_FOUND(11,$submissionId): The ledger configuration could not be retrieved: Cannot validate ledger time."
            )
        }
      }
    }

    "reject a transaction if the ledger effective time is out of bounds" in {
      val nowInstant = Instant.parse("2021-09-01T18:00:00Z")
      val now = Time.Timestamp.assertFromInstant(nowInstant)
      val timeProvider = TimeProvider.Constant(nowInstant)

      val minSkew = Duration.ofSeconds(10)
      val maxSkew = Duration.ofSeconds(30)
      val configuration = Configuration.reasonableInitialConfiguration.copy(
        timeModel = LedgerTimeModel.reasonableDefault.copy(minSkew = minSkew, maxSkew = maxSkew)
      )

      val submissionId = "12345678"

      val transactionLedgerEffectiveTime = now.add(Duration.ofMinutes(5))
      for {
        sqlLedger <- createSqlLedger(
          ledgerId = None,
          participantId = None,
          packages = List.empty,
          timeProvider = timeProvider,
        )
        start = sqlLedger.ledgerEnd()
        _ <- sqlLedger.publishConfiguration(
          maxRecordTime = now.add(Duration.ofMinutes(1)),
          submissionId = "configuration",
          config = configuration,
        )
        result <- sqlLedger.publishTransaction(
          submitterInfo = SubmitterInfo(
            actAs = List(party1),
            applicationId = applicationId,
            commandId = commandId1,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
            ledgerConfiguration = configuration,
          ),
          transactionMeta =
            emptyTransactionMeta(seedService, ledgerEffectiveTime = transactionLedgerEffectiveTime),
          transaction = EmptySubmitted,
        )
        completion <- sqlLedger
          .completions(
            startExclusive = Some(start),
            endInclusive = None,
            applicationId = domain.ApplicationId(applicationId),
            parties = Set(party1),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId1`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.FAILED_PRECONDITION.value)
            status.message should be(
              s"INVALID_LEDGER_TIME(9,$submissionId): Invalid ledger time: Ledger time 2021-09-01T18:05:00Z outside of range [2021-09-01T17:59:50Z, 2021-09-01T18:00:30Z]"
            )
        }
      }
    }
  }

  private val retryStrategy = RetryStrategy.exponentialBackoff(10, 12.millis)

  private def eventually[T](f: => Future[T]): Future[T] = retryStrategy.apply((_, _) => f)

  private def createSqlLedger(): Future[Ledger] =
    createSqlLedger(ledgerId = None, participantId = None, packages = List.empty)

  private def createSqlLedger(ledgerId: String): Future[Ledger] =
    createSqlLedger(ledgerId = ledgerId, packages = List.empty)

  private def createSqlLedger(ledgerId: LedgerId): Future[Ledger] =
    createSqlLedger(ledgerId = Some(ledgerId), participantId = None, packages = List.empty)

  private def createSqlLedger(packages: List[DamlLf.Archive]): Future[Ledger] =
    createSqlLedger(ledgerId = None, participantId = None, packages = packages)

  private def createSqlLedger(ledgerId: String, packages: List[DamlLf.Archive]): Future[Ledger] = {
    val ledgerIdString: String = Ref.LedgerString.assertFromString(ledgerId)
    val assertedLedgerId: LedgerId = LedgerId(ledgerIdString)
    createSqlLedger(ledgerId = Some(assertedLedgerId), participantId = None, packages = packages)
  }

  private def createSqlLedgerWithParticipantId(participantId: ParticipantId): Future[Ledger] =
    createSqlLedger(ledgerId = None, participantId = Some(participantId), packages = List.empty)

  private def createSqlLedger(
      ledgerId: Option[LedgerId],
      participantId: Option[ParticipantId],
      packages: List[DamlLf.Archive],
      timeProvider: TimeProvider = TimeProvider.UTC,
  ): Future[Ledger] = {
    metrics.getNames.forEach(name => { val _ = metrics.remove(name) })
    val ledger =
      new SqlLedger.Owner(
        name = LedgerName(getClass.getSimpleName),
        serverRole = ServerRole.Testing(getClass),
        jdbcUrl = postgresDatabase.url,
        databaseConnectionPoolSize = 16,
        databaseConnectionTimeout = 250.millis,
        providedLedgerId = ledgerId.fold[LedgerIdMode](LedgerIdMode.Dynamic)(LedgerIdMode.Static),
        participantId = participantId.getOrElse(DefaultParticipantId),
        timeProvider = timeProvider,
        packages = InMemoryPackageStore.empty
          .withPackages(Timestamp.Epoch, None, packages)
          .fold(sys.error, identity),
        initialLedgerEntries = ImmArray.Empty,
        queueDepth = queueDepth,
        transactionCommitter = LegacyTransactionCommitter,
        startMode = SqlStartMode.MigrateAndStart,
        eventsPageSize = 100,
        eventsProcessingParallelism = 8,
        acsIdPageSize = 2000,
        acsIdFetchingParallelism = 2,
        acsContractFetchingParallelism = 2,
        servicesExecutionContext = executionContext,
        metrics = new Metrics(metrics),
        lfValueTranslationCache = LfValueTranslationCache.Cache.none,
        engine = new Engine(),
        validatePartyAllocation = false,
        enableCompression = false,
        errorFactories = ErrorFactories(new ErrorCodesVersionSwitcher(true)),
      ).acquire()(ResourceContext(system.dispatcher))
    createdLedgers += ledger
    ledger.asFuture
  }
}

object SqlLedgerSpec {
  private val queueDepth = 128

  private val seedService = SeedService.WeakRandom

  private val ledgerId: LedgerId = LedgerId("TheLedger")

  private val testDar =
    DarParser.assertReadArchiveFromFile(new File(rlocation(ModelTestDar.path)))

  private val DefaultParticipantId = makeParticipantId("test-participant-id")

  private def makeParticipantId(id: String): ParticipantId =
    ParticipantId(Ref.ParticipantId.assertFromString(id))

  private def emptyTransactionMeta(
      seedService: SeedService,
      ledgerEffectiveTime: Time.Timestamp,
  ) = {
    TransactionMeta(
      ledgerEffectiveTime = ledgerEffectiveTime,
      workflowId = None,
      submissionTime = ledgerEffectiveTime,
      submissionSeed = seedService.nextSeed(),
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )
  }

  private object TestIdentifiers {
    val applicationId: Ref.ApplicationId = Ref.ApplicationId.assertFromString("application")
    val party1: Ref.Party = Ref.Party.assertFromString("party1")
    val party2: Ref.Party = Ref.Party.assertFromString("party2")
    val commandId1: Ref.CommandId = Ref.CommandId.assertFromString("commandId1")
    val submissionId1: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submissionId1")
    val submissionId2: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submissionId2")
    val submissionId3: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submissionId3")
  }
}

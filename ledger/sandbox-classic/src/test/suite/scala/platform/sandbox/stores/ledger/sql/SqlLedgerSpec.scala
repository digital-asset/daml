// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import java.io.File
import java.time.{Duration, Instant}
import java.util.UUID

import akka.stream.scaladsl.Sink
import ch.qos.logback.classic.Level
import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.health.Healthy
import com.google.protobuf.any.{Any => AnyProto}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.v2.{SubmissionResult, SubmitterInfo, TransactionMeta}
import com.daml.ledger.resources.{Resource, ResourceContext, TestResourceContext}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarParser
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
import com.daml.platform.store.{IndexMetadata, LfValueTranslationCache}
import com.daml.platform.testing.LogCollector
import com.daml.testing.postgresql.PostgresAroundEach
import com.daml.timer.RetryStrategy
import com.google.rpc.error_details.ErrorInfo
import io.grpc.Status
import org.scalatest.Inside
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec

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
    with MetricsAround {

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
        metadata <- IndexMetadata.read(postgresDatabase.url)
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "allow to resume on an existing participant ID" in {
      val participantId = makeParticipantId("TheParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId)
        _ <- createSqlLedgerWithParticipantId(participantId)
        metadata <- IndexMetadata.read(postgresDatabase.url)
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

    /** Workaround test for asserting that PostgreSQL asynchronous commits are disabled in
      * [[com.daml.platform.store.dao.JdbcLedgerDao]] transactions when used from [[SqlLedger]].
      *
      * NOTE: This is needed for ensuring durability guarantees of Daml-on-SQL.
      */
    "does not use async commit when building JdbcLedgerDao" in {
      for {
        _ <- createSqlLedger(
          ledgerId = None,
          participantId = None,
          packages = List.empty,
          enableAppendOnlySchema = false,
        )
      } yield {
        val hikariDataSourceLogs =
          LogCollector.read[this.type]("com.daml.platform.store.dao.HikariConnection")
        hikariDataSourceLogs should contain(
          Level.INFO -> "Creating Hikari connections with synchronous commit ON"
        )
      }
    }

    "not allow insertion of subsequent parties with the same identifier: operations should succeed without effect" in {
      val party1 = Ref.Party.assertFromString("party1")
      val party2 = Ref.Party.assertFromString("party2")
      val submissionId1 = Ref.SubmissionId.assertFromString("submissionId1")
      val submissionId2 = Ref.SubmissionId.assertFromString("submissionId2")
      val submissionId3 = Ref.SubmissionId.assertFromString("submissionId3")
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
      val now = Time.Timestamp.assertFromInstant(Instant.now())
      val seedService = SeedService.WeakRandom
      val applicationId = Ref.ApplicationId.assertFromString(UUID.randomUUID().toString)
      val party = Ref.Party.assertFromString("party1")
      val commandId = Ref.CommandId.assertFromString("commandId1")
      val submissionId = Ref.SubmissionId.assertFromString("submissionId1")
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
            actAs = List(party),
            applicationId = applicationId,
            commandId = commandId,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = submissionId,
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
            parties = Set(party),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.OK.value)
        }
      }
    }

    "reject a transaction if no configuration is found" in {
      val now = Time.Timestamp.assertFromInstant(Instant.now())
      val seedService = SeedService.WeakRandom
      val applicationId = Ref.ApplicationId.assertFromString(UUID.randomUUID().toString)
      val party = Ref.Party.assertFromString("party1")
      val commandId = Ref.CommandId.assertFromString("commandId1")
      val submissionId = Ref.SubmissionId.assertFromString("submissionId1")
      for {
        sqlLedger <- createSqlLedger()
        start = sqlLedger.ledgerEnd()
        result <- sqlLedger.publishTransaction(
          submitterInfo = SubmitterInfo(
            actAs = List(party),
            applicationId = applicationId,
            commandId = commandId,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = submissionId,
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
            parties = Set(party),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.ABORTED.value)
            status.message should be(
              "No ledger configuration available, cannot validate ledger time"
            )
            status.details should be(
              Seq(
                AnyProto.pack(
                  ErrorInfo.of(
                    reason = "NO_LEDGER_CONFIGURATION",
                    domain = "com.daml.on.sql",
                    metadata = Map.empty,
                  )
                )
              )
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

      val seedService = SeedService.WeakRandom
      val applicationId = Ref.ApplicationId.assertFromString(UUID.randomUUID().toString)
      val party = Ref.Party.assertFromString("party1")
      val commandId = Ref.CommandId.assertFromString("commandId1")
      val submissionId = Ref.SubmissionId.assertFromString("submissionId1")
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
          config = Configuration.reasonableInitialConfiguration.copy(
            timeModel = LedgerTimeModel.reasonableDefault.copy(minSkew = minSkew, maxSkew = maxSkew)
          ),
        )
        result <- sqlLedger.publishTransaction(
          submitterInfo = SubmitterInfo(
            actAs = List(party),
            applicationId = applicationId,
            commandId = commandId,
            deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofHours(1)),
            submissionId = submissionId,
            ledgerConfiguration = Configuration.reasonableInitialConfiguration,
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
            parties = Set(party),
          )
          .runWith(Sink.head)
      } yield {
        result should be(SubmissionResult.Acknowledged)
        completion._1 should be > start
        inside(completion._2.completions) {
          case Seq(Completion(`commandId`, Some(status), _, _, _, _, _)) =>
            status.code should be(Status.Code.ABORTED.value)
            val lowerBound = nowInstant.minus(minSkew)
            val upperBound = nowInstant.plus(maxSkew)
            status.message should be(
              s"Ledger time 2021-09-01T18:05:00Z outside of range [2021-09-01T17:59:50Z, 2021-09-01T18:00:30Z]"
            )
            status.details should be(
              Seq(
                AnyProto.pack(
                  ErrorInfo.of(
                    reason = "INVALID_LEDGER_TIME",
                    domain = "com.daml.on.sql",
                    metadata = Map(
                      "ledgerTime" -> transactionLedgerEffectiveTime.toInstant.toString,
                      "lowerBound" -> lowerBound.toString,
                      "upperBound" -> upperBound.toString,
                    ),
                  )
                )
              )
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
      enableAppendOnlySchema: Boolean = true,
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
          .withPackages(Instant.EPOCH, None, packages)
          .fold(sys.error, identity),
        initialLedgerEntries = ImmArray.empty,
        queueDepth = queueDepth,
        transactionCommitter = LegacyTransactionCommitter,
        startMode = SqlStartMode.MigrateAndStart,
        eventsPageSize = 100,
        eventsProcessingParallelism = 8,
        servicesExecutionContext = executionContext,
        metrics = new Metrics(metrics),
        lfValueTranslationCache = LfValueTranslationCache.Cache.none,
        engine = new Engine(),
        validatePartyAllocation = false,
        enableAppendOnlySchema = enableAppendOnlySchema,
        enableCompression = false,
      ).acquire()(ResourceContext(system.dispatcher))
    createdLedgers += ledger
    ledger.asFuture
  }
}

object SqlLedgerSpec {
  private val queueDepth = 128

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
}

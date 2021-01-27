// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Duration

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{
  Configuration,
  LedgerInitialConditions,
  Offset,
  ReadService,
  TimeModel,
  Update,
}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.lf.data.Bytes
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.MismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer
import com.daml.platform.store.{DbType, FlywayMigrations, IndexMetadata}
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.platform.testing.LogCollector
import com.daml.testing.postgresql.PostgresAroundEach
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

final class JdbcIndexerSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with TestResourceContext
    with AkkaBeforeAndAfterAll
    with PostgresAroundEach {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private def mockedReadService(updates: Seq[(Offset, Update)] = Seq.empty): ReadService = {
    val readService = mock[ReadService]
    when(readService.getLedgerInitialConditions())
      .thenAnswer(
        Source.single(
          LedgerInitialConditions(
            ledgerId = "ledger-id",
            config = Configuration(
              generation = 0,
              timeModel = TimeModel.reasonableDefault,
              maxDeduplicationTime = Duration.ofDays(1),
            ),
            initialRecordTime = Timestamp.Epoch,
          )
        )
      )

    when(readService.stateUpdates(any[Option[Offset]]))
      .thenReturn(Source.fromIterator(() => updates.iterator))

    readService
  }

  private val noOpFlow = Flow[OffsetUpdate].map(_ => ())

  behavior of classOf[JdbcIndexer].getSimpleName

  it should "set the participant id correctly" in {
    val participantId = "the-participant"
    for {
      _ <- runAndShutdownIndexer(participantId)
      metadata <- IndexMetadata.read(postgresDatabase.url)
    } yield metadata.participantId shouldEqual participantId
  }

  it should "allow to resume on the correct participant id" in {
    val participantId = "the-participant"
    for {
      _ <- runAndShutdownIndexer(participantId)
      _ <- runAndShutdownIndexer(participantId)
      metadata <- IndexMetadata.read(postgresDatabase.url)
    } yield metadata.participantId shouldEqual participantId
  }

  it should "not allow to resume on the wrong participant id" in {
    val expectedExisting = "the-participant"
    val expectedProvided = "mismatching-participant-id"
    for {
      _ <- runAndShutdownIndexer(expectedExisting)
      throwable <- runAndShutdownIndexer(expectedProvided).failed
    } yield throwable match {
      case mismatch: MismatchException.ParticipantId =>
        mismatch.existing shouldEqual expectedExisting
        mismatch.provided shouldEqual expectedProvided
      case _ =>
        fail("Did not get the expected exception type", throwable)
    }
  }

  it should "use asynchronous commits with PostgreSQL" in {
    val participantId = "the-participant"
    for {
      indexer <- initializeIndexer(participantId)
      _ = LogCollector.clear[this.type]
      _ <- runAndShutdown(indexer)
    } yield {
      val hikariDataSourceLogs =
        LogCollector.read[this.type]("com.daml.platform.store.dao.HikariConnection")
      hikariDataSourceLogs should contain(
        Level.INFO -> "Creating Hikari connections with asynchronous commit enabled"
      )
    }
  }

  /** This test is agnostic of the PostgreSQL LedgerDao and can be factored out */
  it should "process updates with correct offset steps on subscription" in {
    val participantId = "the-participant"
    val captureBuffer = mutable.ArrayBuffer.empty[OffsetUpdate]
    val flow =
      Flow[OffsetUpdate]
        .wireTap(captureBuffer += _)
        .map(_ => ())

    val mockedUpdates @ Seq(update1, update2, update3) =
      (1 to 3).map(_ => mock[Update])

    val offsets @ Seq(offset1, offset2, offset3) =
      (1 to 3).map(idx => Offset(Bytes.fromByteArray(Array(idx.toByte))))

    val expected = Seq(
      OffsetUpdate(OffsetStep(None, offset1), update1),
      OffsetUpdate(OffsetStep(Some(offset1), offset2), update2),
      OffsetUpdate(OffsetStep(Some(offset2), offset3), update3),
    )

    initializeIndexer(participantId, flow)
      .flatMap {
        _.use {
          _.subscription(mockedReadService(offsets zip mockedUpdates))
            .use(_.completed())
        }
      }
      .map(_ => captureBuffer should contain theSameElementsInOrderAs expected)
  }

  private def runAndShutdown[A](owner: ResourceOwner[A]): Future[Unit] =
    owner.use(_ => Future.unit)

  private def runAndShutdownIndexer(participantId: String): Future[Unit] =
    initializeIndexer(participantId).flatMap(runAndShutdown)

  private def initializeIndexer(
      participantId: String,
      mockFlow: Flow[OffsetUpdate, Unit, NotUsed] = noOpFlow,
  ): Future[ResourceOwner[JdbcIndexer]] = {
    val config = IndexerConfig(
      participantId = v1.ParticipantId.assertFromString(participantId),
      jdbcUrl = postgresDatabase.url,
      startupMode = IndexerStartupMode.MigrateAndStart,
    )
    val metrics = new Metrics(new MetricRegistry)
    val ledgerDaoOwner = JdbcLedgerDao.writeOwner(
      ServerRole.Indexer,
      config.jdbcUrl,
      config.eventsPageSize,
      metrics,
      LfValueTranslation.Cache.none,
      jdbcAsyncCommits = true,
    )
    new indexer.JdbcIndexer.Factory(
      config = config,
      readService = mockedReadService(),
      metrics = metrics,
      updateFlowOwnerBuilder =
        mockedUpdateFlowOwnerBuilder(metrics, config.participantId, mockFlow),
      ledgerDaoOwner = ledgerDaoOwner,
      flywayMigrations = new FlywayMigrations(config.jdbcUrl),
    ).migrateSchema(allowExistingSchema = true)
  }

  private def mockedUpdateFlowOwnerBuilder(
      metrics: Metrics,
      participantId: v1.ParticipantId,
      mockFlow: Flow[OffsetUpdate, Unit, NotUsed],
  ): ExecuteUpdate.FlowOwnerBuilder = {
    val mocked = mock[ExecuteUpdate.FlowOwnerBuilder]
    val executeUpdateMock = mock[ExecuteUpdate]
    when(executeUpdateMock.flow).thenReturn(mockFlow)
    when(
      mocked.apply(
        eqTo(DbType.Postgres),
        any[LedgerDao],
        eqTo(metrics),
        eqTo(participantId),
        any[ExecutionContext],
        any[LoggingContext],
      )
    ).thenReturn(ResourceOwner.successful(executeUpdateMock))
    mocked
  }
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Duration

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerInitialConditions, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.ReadService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, Ref}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.MismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer
import com.daml.platform.store.dao.LedgerDao
import com.daml.platform.store.{DbType, IndexMetadata, LfValueTranslationCache}
import com.daml.platform.testing.LogCollector
import com.daml.testing.postgresql.PostgresAroundEach
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
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

  private def mockedReadService(
      updates: Seq[(Offset, state.Update)] = Seq.empty
  ): state.ReadService = {
    val readService = mock[state.ReadService]
    when(readService.ledgerInitialConditions())
      .thenAnswer(
        Source.single(
          LedgerInitialConditions(
            ledgerId = "ledger-id",
            config = Configuration(
              generation = 0,
              timeModel = LedgerTimeModel.reasonableDefault,
              maxDeduplicationTime = Duration.ofDays(1),
            ),
            initialRecordTime = Timestamp.Epoch,
          )
        )
      )

    when(readService.stateUpdates(any[Option[Offset]])(any[LoggingContext]))
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
    asyncCommitTest(DbType.AsynchronousCommit)
  }

  it should "use local synchronous commits with PostgreSQL when configured to do so" in {
    asyncCommitTest(DbType.LocalSynchronousCommit)
  }

  it should "use synchronous commits with PostgreSQL when configured to do so" in {
    asyncCommitTest(DbType.SynchronousCommit)
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
      (1 to 3).map(_ => mock[state.Update])

    val offsets @ Seq(offset1, offset2, offset3) =
      (1 to 3).map(idx => Offset(Bytes.fromByteArray(Array(idx.toByte))))

    val expected = Seq(
      OffsetUpdate(OffsetStep(None, offset1), update1),
      OffsetUpdate(OffsetStep(Some(offset1), offset2), update2),
      OffsetUpdate(OffsetStep(Some(offset2), offset3), update3),
    )

    initializeIndexer(participantId, mockedReadService(offsets zip mockedUpdates), flow)
      .flatMap {
        _.use {
          _.use(identity)
        }
      }
      .map(_ => captureBuffer should contain theSameElementsInOrderAs expected)
  }

  private def asyncCommitTest(mode: DbType.AsyncCommitMode): Future[Assertion] = {
    val participantId = "the-participant"
    for {
      indexer <- initializeIndexer(participantId, mockedReadService(), jdbcAsyncCommitMode = mode)
      _ = LogCollector.clear[this.type]
      _ <- runAndShutdown(indexer)
    } yield {
      val hikariDataSourceLogs =
        LogCollector.read[this.type]("com.daml.platform.store.dao.HikariConnection")
      hikariDataSourceLogs should contain(
        Level.INFO -> s"Creating Hikari connections with synchronous commit ${mode.setting}"
      )
    }
  }

  private def runAndShutdown[A](owner: ResourceOwner[A]): Future[Unit] =
    owner.use(_ => Future.unit)

  private def runAndShutdownIndexer(participantId: String): Future[Unit] =
    initializeIndexer(participantId, mockedReadService()).flatMap(runAndShutdown)

  private def initializeIndexer(
      participantId: String,
      readService: ReadService,
      mockFlow: Flow[OffsetUpdate, Unit, NotUsed] = noOpFlow,
      jdbcAsyncCommitMode: DbType.AsyncCommitMode = DbType.AsynchronousCommit,
  ): Future[ResourceOwner[Indexer]] = {
    val config = IndexerConfig(
      participantId = Ref.ParticipantId.assertFromString(participantId),
      jdbcUrl = postgresDatabase.url,
      startupMode = IndexerStartupMode.MigrateAndStart,
      asyncCommitMode = jdbcAsyncCommitMode,
      enableAppendOnlySchema = false,
    )
    val metrics = new Metrics(new MetricRegistry)
    StandaloneIndexerServer
      .migrateOnly(
        jdbcUrl = config.jdbcUrl,
        enableAppendOnlySchema = config.enableAppendOnlySchema,
        allowExistingSchema = true,
      )
      .map(_ =>
        new indexer.JdbcIndexer.Factory(
          config = config,
          readService = readService,
          servicesExecutionContext = materializer.executionContext,
          metrics = metrics,
          updateFlowOwnerBuilder =
            mockedUpdateFlowOwnerBuilder(metrics, config.participantId, mockFlow),
          serverRole = ServerRole.Indexer,
          LfValueTranslationCache.Cache.none,
        ).initialized()
      )
  }

  private def mockedUpdateFlowOwnerBuilder(
      metrics: Metrics,
      participantId: Ref.ParticipantId,
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
        any[Int],
        any[ExecutionContext],
        any[LoggingContext],
      )
    ).thenReturn(ResourceOwner.successful(executeUpdateMock))
    mocked
  }
}

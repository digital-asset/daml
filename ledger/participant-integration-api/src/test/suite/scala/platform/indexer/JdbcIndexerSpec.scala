// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Duration

import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{
  Configuration,
  LedgerInitialConditions,
  ReadService,
  TimeModel
}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.MismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.IndexMetadata
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.testing.postgresql.PostgresAroundEach
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

final class JdbcIndexerSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with TestResourceContext
    with AkkaBeforeAndAfterAll
    with PostgresAroundEach {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

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
    } yield
      throwable match {
        case mismatch: MismatchException.ParticipantId =>
          mismatch.existing shouldEqual expectedExisting
          mismatch.provided shouldEqual expectedProvided
        case _ =>
          fail("Did not get the expected exception type", throwable)
      }
  }

  private def runAndShutdown[A](owner: ResourceOwner[A]): Future[Unit] =
    owner.use(_ => Future.unit)

  private def runAndShutdownIndexer(participantId: String): Future[Unit] =
    new JdbcIndexer.Factory(
      ServerRole.Indexer,
      IndexerConfig(
        participantId = v1.ParticipantId.assertFromString(participantId),
        jdbcUrl = postgresDatabase.url,
        startupMode = IndexerStartupMode.MigrateAndStart,
      ),
      mockedReadService,
      new Metrics(new MetricRegistry),
      LfValueTranslation.Cache.none,
    ).migrateSchema(allowExistingSchema = true).flatMap(runAndShutdown)

  private val mockedReadService: ReadService =
    when(mock[ReadService].getLedgerInitialConditions())
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
          ))
      )
      .getMock[ReadService]

}

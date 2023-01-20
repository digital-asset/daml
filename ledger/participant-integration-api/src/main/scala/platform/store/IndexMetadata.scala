// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.platform.ApiOffset
import com.daml.platform.configuration.{
  AcsStreamsConfig,
  ServerRole,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
}
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerReadDao}
import com.daml.platform.store.interning.StringInterningView
import io.opentelemetry.api.GlobalOpenTelemetry
import scalaz.Tag

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object IndexMetadata {

  def read(
      jdbcUrl: String
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexMetadata] = {
    for {
      dao <- ownDao(jdbcUrl)
      matadata <- ResourceOwner.forFuture(() => metadata(dao))
    } yield matadata
  }

  private def metadata(dao: LedgerReadDao)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[IndexMetadata] = {
    for {
      ledgerId <- dao.lookupLedgerId()
      participantId <- dao.lookupParticipantId()
      ledgerEnd <- ledgerId match {
        case Some(_) => dao.lookupLedgerEnd().map(x => Some(x.lastOffset))
        case None => Future.successful(None)
      }
    } yield metadata(ledgerId, participantId, ledgerEnd)
  }

  private def ownDao(
      jdbcUrl: String
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ) = {
    val metrics = new Metrics(
      new DropwizardMetricsFactory(new MetricRegistry),
      new OpenTelemetryFactory(GlobalOpenTelemetry.getMeter("daml")),
    )
    DbSupport
      .owner(
        serverRole = ServerRole.ReadIndexMetadata,
        metrics = metrics,
        dbConfig = DbConfig(
          jdbcUrl = jdbcUrl,
          connectionPool = ConnectionPoolConfig(
            connectionPoolSize = 1,
            connectionTimeout = 250.millis,
          ),
        ),
      )
      .map(dbSupport =>
        JdbcLedgerDao.read(
          dbSupport = dbSupport,
          eventsProcessingParallelism = 8,
          completionsPageSize = 1000,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          engine = None,
          participantId = Ref.ParticipantId.assertFromString("1"),
          ledgerEndCache = MutableLedgerEndCache(), // not used
          stringInterning = new StringInterningView(), // not used
          acsStreamsConfig = AcsStreamsConfig.default,
          transactionFlatStreamsConfig = TransactionFlatStreamsConfig.default,
          transactionTreeStreamsConfig = TransactionTreeStreamsConfig.default,
          globalMaxEventIdQueries = 20,
          globalMaxEventPayloadQueries = 10,
        )
      )
  }

  private val Empty = "<empty>"

  private def metadata(
      ledgerId: Option[LedgerId],
      participantId: Option[ParticipantId],
      ledgerEnd: Option[Offset],
  ): IndexMetadata =
    IndexMetadata(
      ledgerId = ledgerId.fold(Empty)(Tag.unwrap),
      participantId = participantId.fold(Empty)(Tag.unwrap),
      ledgerEnd = ledgerEnd.fold(Empty)(ApiOffset.toApiString),
      participantIntegrationApiVersion = BuildInfo.Version,
    )

}

final case class IndexMetadata private (
    ledgerId: String,
    participantId: String,
    ledgerEnd: String,
    participantIntegrationApiVersion: String,
)

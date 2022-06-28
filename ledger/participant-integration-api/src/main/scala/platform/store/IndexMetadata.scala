// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.dao.JdbcLedgerDao
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.interning.StringInterningView
import scalaz.Tag

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object IndexMetadata {

  def read(
      jdbcUrl: String
  )(implicit
      resourceContext: ResourceContext,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[IndexMetadata] =
    ownDao(jdbcUrl).use { dao =>
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
    val metrics = new Metrics(new MetricRegistry)
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
          eventsPageSize = 1000,
          eventsProcessingParallelism = 8,
          acsIdPageSize = 20000,
          acsIdPageBufferSize = 1,
          acsIdPageWorkingMemoryBytes = 100 * 1024 * 1024,
          acsIdFetchingParallelism = 2,
          acsContractFetchingParallelism = 2,
          acsGlobalParallelism = 10,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = None,
          participantId = Ref.ParticipantId.assertFromString("1"),
          ledgerEndCache = MutableLedgerEndCache(), // not used
          stringInterning = new StringInterningView(), // not used
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

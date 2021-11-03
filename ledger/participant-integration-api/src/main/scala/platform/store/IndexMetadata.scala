// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.platform.server.api.validation.ErrorFactories
import scalaz.Tag

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object IndexMetadata {

  def read(
      jdbcUrl: String,
      errorFactories: ErrorFactories,
  )(implicit
      resourceContext: ResourceContext,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[IndexMetadata] =
    ownDao(jdbcUrl, errorFactories).use { dao =>
      for {
        ledgerId <- dao.lookupLedgerId()
        participantId <- dao.lookupParticipantId()
        ledgerEnd <- dao.lookupInitialLedgerEnd()
      } yield metadata(ledgerId, participantId, ledgerEnd)
    }

  private def ownDao(
      jdbcUrl: String,
      errorFactories: ErrorFactories,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ) =
    com.daml.platform.store.appendonlydao.JdbcLedgerDao.readOwner(
      serverRole = ServerRole.ReadIndexMetadata,
      jdbcUrl = jdbcUrl,
      connectionPoolSize = 1,
      connectionTimeout = 250.millis,
      eventsPageSize = 1000,
      eventsProcessingParallelism = 8,
      servicesExecutionContext = executionContext,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslationCache.Cache.none,
      enricher = None,
      // No participant ID is available for the dump index meta path,
      // and this property is not needed for the used ReadDao.
      participantId = Ref.ParticipantId.assertFromString("1"),
      errorFactories = errorFactories,
    )

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

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.ResourceContext
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.JdbcLedgerDao
import com.daml.platform.store.dao.events.LfValueTranslation
import scalaz.Tag

import scala.concurrent.{ExecutionContext, Future}

object IndexMetadata {

  def read(jdbcUrl: String)(
      implicit resourceContext: ResourceContext,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[IndexMetadata] =
    ownDao(jdbcUrl).use { dao =>
      for {
        ledgerId <- dao.lookupLedgerId()
        participantId <- dao.lookupParticipantId()
        ledgerEnd <- dao.lookupInitialLedgerEnd()
      } yield metadata(ledgerId, participantId, ledgerEnd)
    }

  private def ownDao(jdbcUrl: String)(implicit loggingContext: LoggingContext) =
    JdbcLedgerDao.readOwner(
      serverRole = ServerRole.ReadIndexMetadata,
      jdbcUrl = jdbcUrl,
      eventsPageSize = 1000,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslation.Cache.none,
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

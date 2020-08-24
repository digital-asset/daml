// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.Offset
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
      implicit executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Option[IndexMetadata]] =
    ownDao(jdbcUrl).use { dao =>
      for {
        ledgerId <- dao.lookupLedgerId()
        ledgerEnd <- dao.lookupInitialLedgerEnd()
      } yield metadata(ledgerId, ledgerEnd)
    }

  private def ownDao(jdbcUrl: String)(implicit loggingContext: LoggingContext) =
    JdbcLedgerDao.readOwner(
      serverRole = ServerRole.ReadIndexMetadata,
      jdbcUrl = jdbcUrl,
      eventsPageSize = 1000,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslation.Cache.none,
    )

  private def metadata(
      ledgerId: Option[LedgerId],
      ledgerEnd: Option[Offset],
  ): Option[IndexMetadata] =
    for {
      id <- ledgerId
      end <- ledgerEnd
    } yield IndexMetadata(Tag.unwrap(id), ApiOffset.toApiString(end), BuildInfo.Version)

}

final case class IndexMetadata private (
    ledgerId: String,
    ledgerEnd: String,
    participantIntegrationApiVersion: String,
)

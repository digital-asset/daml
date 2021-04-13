// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.Materializer
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.LfValueTranslationCache

import scala.concurrent.ExecutionContext

private[platform] object JdbcIndex {
  def owner(
      serverRole: ServerRole,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      databaseConnectionPoolSize: Int,
      eventsPageSize: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      enableAppendOnlySchema: Boolean,
  )(implicit mat: Materializer, loggingContext: LoggingContext): ResourceOwner[IndexService] =
    new ReadOnlySqlLedger.Owner(
      serverRole = serverRole,
      jdbcUrl = jdbcUrl,
      databaseConnectionPoolSize = databaseConnectionPoolSize,
      initialLedgerId = ledgerId,
      eventsPageSize = eventsPageSize,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      enricher = enricher,
      enableAppendOnlySchema = enableAppendOnlySchema,
    ).map { ledger =>
      new LedgerBackedIndexService(MeteredReadOnlyLedger(ledger, metrics), participantId)
    }
}

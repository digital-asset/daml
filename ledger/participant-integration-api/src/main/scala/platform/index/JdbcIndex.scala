// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.Materializer
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.LfValueTranslationCache

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private[platform] object JdbcIndex {
  def owner(
      serverRole: ServerRole,
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      jdbcUrl: String,
      databaseConnectionPoolSize: Int,
      databaseConnectionTimeout: FiniteDuration,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      enableMutableContractStateCache: Boolean,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
      enableSelfServiceErrorCodes: Boolean,
  )(implicit mat: Materializer, loggingContext: LoggingContext): ResourceOwner[IndexService] =
    new ReadOnlySqlLedger.Owner(
      serverRole = serverRole,
      jdbcUrl = jdbcUrl,
      databaseConnectionPoolSize = databaseConnectionPoolSize,
      databaseConnectionTimeout = databaseConnectionTimeout,
      initialLedgerId = ledgerId,
      eventsPageSize = eventsPageSize,
      eventsProcessingParallelism = eventsProcessingParallelism,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      enricher = enricher,
      maxContractStateCacheSize = maxContractStateCacheSize,
      maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
      enableMutableContractStateCache = enableMutableContractStateCache,
      enableInMemoryFanOutForLedgerApi = enableInMemoryFanOutForLedgerApi,
      participantId = participantId,
      maxTransactionsInMemoryFanOutBufferSize = maxTransactionsInMemoryFanOutBufferSize,
      errorFactories = ErrorFactories(new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes)),
    ).map { ledger =>
      new LedgerBackedIndexService(
        MeteredReadOnlyLedger(ledger, metrics),
        participantId,
        errorFactories = ErrorFactories(new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes)),
      )
    }
}

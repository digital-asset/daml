// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.Materializer
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{ReadOnlyLedgerImpl, DbSupport, LfValueTranslationCache}

import scala.concurrent.ExecutionContext

private[platform] object JdbcIndex {
  def owner(
      dbSupport: DbSupport,
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
      acsIdQueueLimit: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
  )(implicit mat: Materializer, loggingContext: LoggingContext): ResourceOwner[IndexService] =
    new ReadOnlyLedgerImpl.Owner(
      dbSupport = dbSupport,
      initialLedgerId = ledgerId,
      eventsPageSize = eventsPageSize,
      eventsProcessingParallelism = eventsProcessingParallelism,
      acsIdPageSize = acsIdPageSize,
      acsIdFetchingParallelism = acsIdFetchingParallelism,
      acsContractFetchingParallelism = acsContractFetchingParallelism,
      acsGlobalParallelism = acsGlobalParallelism,
      acsIdQueueLimit = acsIdQueueLimit,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      enricher = enricher,
      maxContractStateCacheSize = maxContractStateCacheSize,
      maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
      enableInMemoryFanOutForLedgerApi = enableInMemoryFanOutForLedgerApi,
      participantId = participantId,
      maxTransactionsInMemoryFanOutBufferSize = maxTransactionsInMemoryFanOutBufferSize,
      errorFactories = ErrorFactories(),
    ).map { ledger =>
      new LedgerBackedIndexService(
        MeteredReadOnlyLedger(ledger, metrics),
        participantId,
        errorFactories = ErrorFactories(),
      )
    }
}

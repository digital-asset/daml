// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.config.IndexServiceConfig
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

object LedgerApiServer {
  def createInMemoryStateAndUpdater(
      participantId: Ref.ParticipantId,
      commandProgressTracker: CommandProgressTracker,
      indexServiceConfig: IndexServiceConfig,
      maxCommandsInFlight: Int,
      metrics: LedgerApiServerMetrics,
      executionContext: ExecutionContext,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
  )(
      mutableLedgerEndCache: MutableLedgerEndCache,
      stringInterningView: StringInterningView,
  )(implicit
      traceContext: TraceContext
  ): ResourceOwner[(InMemoryState, InMemoryStateUpdater.UpdaterFlow)] =
    for {
      inMemoryState <- InMemoryState.owner(
        participantId = participantId,
        commandProgressTracker = commandProgressTracker,
        apiStreamShutdownTimeout = indexServiceConfig.apiStreamShutdownTimeout,
        bufferedStreamsPageSize = indexServiceConfig.bufferedStreamsPageSize,
        maxContractStateCacheSize = indexServiceConfig.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = indexServiceConfig.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          indexServiceConfig.maxTransactionsInMemoryFanOutBufferSize,
        executionContext = executionContext,
        maxCommandsInFlight = maxCommandsInFlight,
        metrics = metrics,
        tracer = tracer,
        loggerFactory = loggerFactory,
      )(mutableLedgerEndCache, stringInterningView)

      inMemoryStateUpdater <- InMemoryStateUpdater.owner(
        inMemoryState = inMemoryState,
        prepareUpdatesParallelism = indexServiceConfig.inMemoryStateUpdaterParallelism,
        preparePackageMetadataTimeOutWarning =
          indexServiceConfig.preparePackageMetadataTimeOutWarning.underlying,
        offsetCheckpointCacheUpdateInterval =
          indexServiceConfig.offsetCheckpointCacheUpdateInterval.underlying,
        metrics = metrics,
        loggerFactory = loggerFactory,
      )
    } yield inMemoryState -> inMemoryStateUpdater
}

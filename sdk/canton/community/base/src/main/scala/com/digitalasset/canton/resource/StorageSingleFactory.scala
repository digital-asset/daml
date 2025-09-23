// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.lifecycle.{CloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

/** Storage factory for nodes with a single writer to the database. MUST NOT be used for replicated
  * nodes, use [[StorageMultiFactory]] instead.
  */
class StorageSingleFactory(
    val config: StorageConfig
) extends StorageFactory {

  def create(
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, Storage] =
    config match {
      case StorageConfig.Memory(_, _) =>
        EitherT.rightT(new MemoryStorage(loggerFactory, timeouts))
      case dbConfig: DbConfig =>
        DbStorageSingle
          .create(
            dbConfig,
            connectionPoolForParticipant,
            logQueryCost,
            clock,
            scheduler,
            metrics,
            timeouts,
            loggerFactory,
          )
          .widen[Storage]
    }
}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import com.digitalasset.canton.config.*
import com.digitalasset.canton.lifecycle.{CloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.StorageFactory.StorageCreationException
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

trait StorageFactory {

  /** Throws an exception in case of errors or shutdown during storage creation. */
  def tryCreate(
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
  ): Storage =
    create(
      connectionPoolForParticipant,
      logQueryCost,
      clock,
      scheduler,
      metrics,
      timeouts,
      loggerFactory,
    )
      .valueOr(err => throw new StorageCreationException(err))
      .onShutdown(throw new StorageCreationException("Shutdown during storage creation"))

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
  ): EitherT[UnlessShutdown, String, Storage]
}

object StorageFactory {
  class StorageCreationException(message: String) extends RuntimeException(message)
}

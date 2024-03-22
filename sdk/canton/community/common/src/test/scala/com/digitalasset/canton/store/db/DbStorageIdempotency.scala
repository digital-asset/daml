// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction.{All, ReadTransactional}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** DbStorage instance for idempotency testing where we run each write action twice. */
class DbStorageIdempotency(
    val underlying: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override protected implicit val ec: ExecutionContext)
    extends DbStorage
    with NamedLogging {
  override def threadsAvailableForWriting: PositiveInt = underlying.threadsAvailableForWriting
  override val profile: DbStorage.Profile = underlying.profile
  override def metrics: DbStorageMetrics = underlying.metrics
  override val dbConfig: DbConfig = underlying.dbConfig
  override protected val logOperations: Boolean = false

  override protected[canton] def runRead[A](
      action: ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    underlying.runRead(action, operationName, maxRetries)

  override protected[canton] def runWrite[A](
      action: All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    underlying.runWrite(action, operationName + "-1", maxRetries).flatMap { _ =>
      underlying.runWrite(action, operationName + "-2", maxRetries)
    }

  override def isActive: Boolean = underlying.isActive
}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

class StorageMultiFactory(
    val config: StorageConfig,
    exitOnFatalFailures: Boolean,
    replicationConfig: Option[ReplicationConfig],
    onActive: () => FutureUnlessShutdown[Unit],
    onPassive: () => FutureUnlessShutdown[Option[CloseContext]],
    mainLockCounter: DbLockCounter,
    poolLockCounter: DbLockCounter,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
    initialSessionContext: Option[CloseContext] = None,
) extends StorageFactory
    with NamedLogging {

  override def create(
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
        replicationConfig match {
          case Some(replConfig) if replConfig.isEnabled =>
            val writePoolSize =
              dbConfig.numWriteConnectionsCanton(
                connectionPoolForParticipant,
                withWriteConnectionPool = true,
                withMainConnection = true,
              )
            val readPoolSize =
              dbConfig.numReadConnectionsCanton(
                forParticipant = connectionPoolForParticipant,
                withWriteConnectionPool = true,
                withMainConnection = false,
              )

            DbStorageMulti
              .create(
                dbConfig,
                replConfig.connectionPool,
                readPoolSize,
                writePoolSize,
                mainLockCounter,
                poolLockCounter,
                onActive,
                onPassive,
                metrics,
                logQueryCost,
                None,
                scheduler,
                timeouts,
                exitOnFatalFailures = exitOnFatalFailures,
                futureSupervisor,
                loggerFactory,
                initialSessionContext,
              )
              .widen[Storage]
          case _ =>
            logger.info(s"Replication is disabled, using DbStorageSingle")
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

}

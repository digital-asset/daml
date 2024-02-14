// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.timer.RetryStrategy
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.tracing.TraceContext

import javax.sql.DataSource
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Using}

/** Returns a DataSource that is guaranteed to be connected to a responsive, compatible database. */
object VerifiedDataSource {

  private val MaxInitialConnectRetryAttempts: Int = 600

  def apply(jdbcUrl: String, loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DataSource] = {
    val dataSourceStorageBackend =
      StorageBackendFactory
        .of(dbType = DbType.jdbcType(jdbcUrl), loggerFactory = loggerFactory)
        .createDataSourceStorageBackend
    apply(
      dataSourceStorageBackend,
      DataSourceStorageBackend.DataSourceConfig(jdbcUrl),
      loggerFactory,
    )
  }

  def apply(
      dataSourceStorageBackend: DataSourceStorageBackend,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DataSource] = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    for {
      dataSource <- RetryStrategy.constant(
        attempts = MaxInitialConnectRetryAttempts,
        waitTime = 1.second,
      ) { (i, _) =>
        Future {
          val createdDatasource =
            dataSourceStorageBackend.createDataSource(dataSourceConfig, loggerFactory)
          logger.info(
            s"Attempting to connect to the database (attempt $i/$MaxInitialConnectRetryAttempts)"
          )
          Using.resource(createdDatasource.getConnection)(
            dataSourceStorageBackend.checkDatabaseAvailable
          )
          createdDatasource
        }.andThen { case Failure(exception) =>
          logger.warn(exception.getMessage)
        }
      }
      _ <- Future {
        Using.resource(dataSource.getConnection)(
          dataSourceStorageBackend.checkCompatibility
        )
      }
    } yield dataSource
  }

}

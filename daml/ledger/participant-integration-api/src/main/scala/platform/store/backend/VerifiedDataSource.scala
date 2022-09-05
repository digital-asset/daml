// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.DbType
import com.daml.timer.RetryStrategy
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Using}

/** Returns a DataSource that is guaranteed to be connected to a responsive, compatible database. */
object VerifiedDataSource {

  private val MaxInitialConnectRetryAttempts: Int = 600

  private val logger = ContextualizedLogger.get(this.getClass)

  def apply(jdbcUrl: String)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[DataSource] = {
    val dataSourceStorageBackend =
      StorageBackendFactory
        .of(DbType.jdbcType(jdbcUrl))
        .createDataSourceStorageBackend
    for {
      dataSource <- RetryStrategy.constant(
        attempts = MaxInitialConnectRetryAttempts,
        waitTime = 1.second,
      ) { (i, _) =>
        Future {
          val createdDatasource = dataSourceStorageBackend.createDataSource(
            DataSourceStorageBackend.DataSourceConfig(jdbcUrl)
          )
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

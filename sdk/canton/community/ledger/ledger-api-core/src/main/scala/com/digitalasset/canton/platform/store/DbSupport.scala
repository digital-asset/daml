// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.{
  DataSourceStorageBackend,
  StorageBackendFactory,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher

import scala.concurrent.duration.FiniteDuration

final case class DbSupport(
    dbDispatcher: DbDispatcher with ReportsHealth,
    storageBackendFactory: StorageBackendFactory,
)

object DbSupport {

  final case class ParticipantDataSourceConfig(jdbcUrl: String)

  final case class DataSourceProperties(
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def createDbConfig(config: ParticipantDataSourceConfig): DbConfig = DbConfig(
      jdbcUrl = config.jdbcUrl,
      connectionPool = connectionPool,
      postgres = postgres,
    )
  }

  final case class ConnectionPoolConfig(
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
  )

  final case class DbConfig(
      jdbcUrl: String,
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def dataSourceConfig: DataSourceStorageBackend.DataSourceConfig =
      DataSourceStorageBackend.DataSourceConfig(
        jdbcUrl = jdbcUrl,
        postgresConfig = postgres,
      )
  }

  def owner(
      dbConfig: DbConfig,
      serverRole: ServerRole,
      metrics: Metrics,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[DbSupport] = {
    val dbType = DbType.jdbcType(dbConfig.jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType, loggerFactory)
    DbDispatcher
      .owner(
        dataSource = storageBackendFactory.createDataSourceStorageBackend
          .createDataSource(dbConfig.dataSourceConfig, loggerFactory),
        serverRole = serverRole,
        connectionPoolSize = dbConfig.connectionPool.connectionPoolSize,
        connectionTimeout = dbConfig.connectionPool.connectionTimeout,
        metrics = metrics,
        loggerFactory = loggerFactory,
      )
      .map(dbDispatcher =>
        DbSupport(
          dbDispatcher = dbDispatcher,
          storageBackendFactory = storageBackendFactory,
        )
      )
  }
}

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.backend.{DataSourceStorageBackend, StorageBackendFactory}
import com.daml.resources.{PureResource, Resource}

import scala.concurrent.duration.FiniteDuration

case class DbSupport(
    dbDispatcher: DbDispatcher with ReportsHealth,
    storageBackendFactory: StorageBackendFactory,
)

object DbSupport {

  case class ParticipantDataSourceConfig(jdbcUrl: String)

  case class DataSourceProperties(
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def createDbConfig(config: ParticipantDataSourceConfig): DbConfig = DbConfig(
      jdbcUrl = config.jdbcUrl,
      connectionPool = connectionPool,
      postgres = postgres,
    )
  }

  case class ConnectionPoolConfig(
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
  )

  case class DbConfig(
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
  )(implicit loggingContext: LoggingContext): ResourceOwner[DbSupport] = {
    val dbType = DbType.jdbcType(dbConfig.jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    DbDispatcher
      .owner(
        dataSource = storageBackendFactory.createDataSourceStorageBackend
          .createDataSource(dbConfig.dataSourceConfig),
        serverRole = serverRole,
        connectionPoolSize = dbConfig.connectionPool.connectionPoolSize,
        connectionTimeout = dbConfig.connectionPool.connectionTimeout,
        metrics = metrics,
      )
      .map(dbDispatcher =>
        DbSupport(
          dbDispatcher = dbDispatcher,
          storageBackendFactory = storageBackendFactory,
        )
      )
  }

  def migratedOwner(
      serverRole: ServerRole,
      dbConfig: DbConfig,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[DbSupport] = {
    val migrationOwner = new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[ResourceContext, Unit] =
        PureResource(new FlywayMigrations(dbConfig.jdbcUrl).migrate())
    }

    for {
      _ <- migrationOwner
      dbSupport <- owner(
        serverRole = serverRole,
        metrics = metrics,
        dbConfig = dbConfig,
      )
    } yield dbSupport
  }
}

// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  case class ConnectionPoolConfig(
      minimumIdle: Int,
      maxPoolSize: Int,
      connectionTimeout: FiniteDuration,
  )

  case class DbConfig(
      jdbcUrl: String,
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def dataSourceConfig = DataSourceStorageBackend.DataSourceConfig(
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
        minimumIdle = dbConfig.connectionPool.minimumIdle,
        maxPoolSize = dbConfig.connectionPool.maxPoolSize,
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
        PureResource(new FlywayMigrations(dbConfig.dataSourceConfig).migrate())
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

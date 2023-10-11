// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.DataSourceStorageBackend
import com.digitalasset.canton.platform.store.backend.common.InitHookDataSourceProxy

import java.sql.Connection
import javax.sql.DataSource

object OracleDataSourceStorageBackend extends DataSourceStorageBackend {
  override def createDataSource(
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
      connectionInitHook: Option[Connection => Unit],
  ): DataSource = {
    val oracleDataSource = new oracle.jdbc.pool.OracleDataSource
    oracleDataSource.setURL(dataSourceConfig.jdbcUrl)
    InitHookDataSourceProxy(oracleDataSource, connectionInitHook.toList, loggerFactory)
  }

  override def checkDatabaseAvailable(connection: Connection): Unit =
    assert(SQL"SELECT 1 FROM DUAL".as(get[Int](1).single)(connection) == 1)
}

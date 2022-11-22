// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.postgres

import java.sql.Connection
import com.daml.logging.LoggingContext
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.{DataSourceStorageBackend, StorageBackendFactory}
import com.daml.testing.postgresql.PostgresAroundEach

import javax.sql.DataSource
import org.scalatest.Suite

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** Creates a fresh data source and connection for each test case
  */
trait PostgresConnectionSupport extends PostgresAroundEach {
  self: Suite =>

  implicit var conn: Connection = _
  implicit val dbType: DbType = DbType.Postgres
  private val dataSourceBackend = StorageBackendFactory.of(dbType).createDataSourceStorageBackend
  implicit var dataSource: DataSource = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    dataSource = dataSourceBackend.createDataSource(
      dataSourceConfig = DataSourceStorageBackend.DataSourceConfig(postgresDatabase.url)
    )(LoggingContext.ForTesting)
    conn = retry(20, 1000) {
      val c = dataSource.getConnection
      dataSourceBackend.checkDatabaseAvailable(c)
      c
    }
  }

  override protected def afterEach(): Unit = {
    conn.close()
    super.afterEach()
  }

  @tailrec
  private def retry[T](max: Int, sleep: Long)(t: => T): T =
    Try(t) match {
      case Success(value) => value
      case Failure(_) if max > 0 =>
        Thread.sleep(sleep)
        retry(max - 1, sleep)(t)
      case Failure(exception) => throw exception
    }
}

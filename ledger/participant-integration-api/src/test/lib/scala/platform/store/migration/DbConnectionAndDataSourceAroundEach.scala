// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration

import java.sql.Connection
import javax.sql.DataSource

import com.daml.logging.LoggingContext
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.{DataSourceStorageBackend, StorageBackendFactory}
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait DbConnectionAroundEachBase {
  implicit def connection: Connection
  implicit def dbType: DbType
  implicit def dataSource: DataSource
  protected def jdbcUrl: String
}

trait DbConnectionAndDataSourceAroundEach
    extends BeforeAndAfterEach
    with DbConnectionAroundEachBase {
  self: Suite =>

  implicit var connection: Connection = _

  private val dataSourceBackend = StorageBackendFactory.of(dbType).createDataSourceStorageBackend
  implicit var dataSource: DataSource = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    dataSource = dataSourceBackend.createDataSource(
      dataSourceConfig = DataSourceStorageBackend.DataSourceConfig(jdbcUrl)
    )(LoggingContext.ForTesting)
    connection = retry(20, 1000) {
      val c = dataSource.getConnection
      dataSourceBackend.checkDatabaseAvailable(c)
      c
    }
  }

  override protected def afterEach(): Unit = {
    connection.close()
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

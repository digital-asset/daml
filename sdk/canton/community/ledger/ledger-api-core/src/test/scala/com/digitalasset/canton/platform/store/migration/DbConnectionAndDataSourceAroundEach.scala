// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.backend.{
  DataSourceStorageBackend,
  StorageBackendFactory,
}
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.sql.Connection
import javax.sql.DataSource
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
    with DbConnectionAroundEachBase
    with BaseTest {
  self: Suite =>

  implicit var connection: Connection = _

  private val dataSourceBackend =
    StorageBackendFactory.of(dbType, loggerFactory).createDataSourceStorageBackend
  implicit var dataSource: DataSource = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    dataSource = dataSourceBackend.createDataSource(
      dataSourceConfig = DataSourceStorageBackend.DataSourceConfig(jdbcUrl),
      loggerFactory = loggerFactory,
    )
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
        Threading.sleep(sleep)
        retry(max - 1, sleep)(t)
      case Failure(exception) => throw exception
    }
}

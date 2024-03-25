// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.dbutils
import com.daml.dbutils.ConnectionPool
import dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.Inside
import org.scalatest.AsyncTestSuite
import org.scalatest.matchers.should.Matchers

trait HttpServicePostgresInt extends AbstractHttpServiceIntegrationTestFuns with PostgresAroundAll {
  this: AsyncTestSuite with Matchers with Inside =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)
  protected implicit val metrics: HttpJsonApiMetrics = HttpJsonApiMetrics.ForTesting

  // has to be lazy because jdbcConfig_ is NOT initialized yet
  protected lazy val dao = dbbackend.ContractDao(jdbcConfig_)

  // has to be lazy because postgresFixture is NOT initialized yet
  protected[this] def jdbcConfig_ = HttpServicePostgresInt.defaultJdbcConfig(postgresDatabase.url)

  override protected def afterAll(): Unit = {
    dao.close()
    super.afterAll()
  }
}

object HttpServicePostgresInt {
  def defaultJdbcConfig(url: => String) = JdbcConfig(
    dbutils.JdbcConfig(
      driver = "org.postgresql.Driver",
      url = url,
      user = "test",
      password = "",
      tablePrefix = "some_nice_prefix_",
      poolSize = ConnectionPool.PoolSize.Integration,
    ),
    startMode = DbStartupMode.CreateOnly,
  )
}

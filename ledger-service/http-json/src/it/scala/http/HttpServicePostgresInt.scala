// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.ConnectionPool.PoolSize
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.Inside
import org.scalatest.AsyncTestSuite
import org.scalatest.matchers.should.Matchers

trait HttpServicePostgresInt extends AbstractHttpServiceIntegrationTestFuns with PostgresAroundAll {
  this: AsyncTestSuite with Matchers with Inside =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // has to be lazy because jdbcConfig_ is NOT initialized yet
  protected lazy val dao = dbbackend.ContractDao(jdbcConfig_, poolSize = PoolSize.Integration)

  // has to be lazy because postgresFixture is NOT initialized yet
  protected[this] def jdbcConfig_ = PostgresIntTest.defaultJdbcConfig(postgresDatabase.url)

  override protected def afterAll(): Unit = {
    dao.close()
    super.afterAll()
  }
}

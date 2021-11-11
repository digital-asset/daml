// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.ConnectionPool
import org.scalatest.Inside
import org.scalatest.AsyncTestSuite
import org.scalatest.matchers.should.Matchers

trait HttpServiceOracleInt extends AbstractHttpServiceIntegrationTestFuns {
  this: AsyncTestSuite with Matchers with Inside =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  import sys.env
  private[this] def oraclePort = env("ORACLE_PORT")
  private[this] def oraclePwd = env("ORACLE_PWD")
  private[this] def oracleUsername = env("ORACLE_USERNAME")

  protected[this] lazy val jdbcConfig_ = JdbcConfig(
    driver = "oracle.jdbc.OracleDriver",
    url = s"jdbc:oracle:thin:@//localhost:$oraclePort/XEPDB1",
    user = oracleUsername,
    password = oraclePwd,
    poolSize = ConnectionPool.PoolSize.Integration,
    createSchema = true,
  )
}

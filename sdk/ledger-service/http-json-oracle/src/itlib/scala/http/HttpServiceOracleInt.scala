// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.dbutils
import dbutils.ConnectionPool
import com.daml.testing.oracle.OracleAroundAll
import dbbackend.{DbStartupMode, JdbcConfig}
import dbbackend.OracleQueries.DisableContractPayloadIndexing

import org.scalatest.Inside
import org.scalatest.AsyncTestSuite
import org.scalatest.matchers.should.Matchers

trait HttpServiceOracleInt extends AbstractHttpServiceIntegrationTestFuns with OracleAroundAll {
  this: AsyncTestSuite with Matchers with Inside =>

  protected def disableContractPayloadIndexing: DisableContractPayloadIndexing

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  protected[this] def jdbcConfig_ =
    HttpServiceOracleInt.defaultJdbcConfig(
      oracleJdbcUrlWithoutCredentials,
      oracleUserName,
      oracleUserPwd,
      disableContractPayloadIndexing = disableContractPayloadIndexing,
    )
}

object HttpServiceOracleInt {
  def defaultJdbcConfig(
      url: => String,
      user: => String,
      pwd: => String,
      disableContractPayloadIndexing: DisableContractPayloadIndexing = false,
  ) = JdbcConfig(
    dbutils.JdbcConfig(
      driver = "oracle.jdbc.OracleDriver",
      url = url,
      user = user,
      password = pwd,
      tablePrefix = "some_nice_prefix_",
      poolSize = ConnectionPool.PoolSize.Integration,
    ),
    startMode = DbStartupMode.CreateOnly,
    backendSpecificConf =
      if (disableContractPayloadIndexing) Map(DisableContractPayloadIndexing -> "true")
      else Map.empty,
  )
}

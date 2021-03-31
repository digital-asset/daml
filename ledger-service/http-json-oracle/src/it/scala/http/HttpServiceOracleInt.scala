// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.testing.oracle.OracleAroundAll

import org.scalatest.Inside
import org.scalatest.AsyncTestSuite
import org.scalatest.matchers.should.Matchers

trait HttpServiceOracleInt extends AbstractHttpServiceIntegrationTestFuns with OracleAroundAll {
  this: AsyncTestSuite with Matchers with Inside =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  protected[this] lazy val jdbcConfig_ = JdbcConfig(
    driver = "oracle.jdbc.OracleDriver",
    url = oracleJdbcUrl,
    user = oracleUser,
    password = oraclePwd,
    createSchema = true,
  )
}

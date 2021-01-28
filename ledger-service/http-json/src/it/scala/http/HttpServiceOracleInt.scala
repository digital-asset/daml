// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

trait HttpServiceOracleInt extends AbstractHttpServiceIntegrationTestFuns {
  this: AsyncFreeSpec with Matchers with Inside with StrictLogging =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // has to be lazy because postgresFixture is NOT initialized yet
  protected[this] lazy val jdbcConfig_ = JdbcConfig(
    driver = "oracle.jdbc.OracleDriver",
    url = "oracle.jdbc.OracleDriver,url=jdbc:oracle:thin:@//localhost:1521/XEPDB1",
    user = "someuser",
    password = "verysecret",
    createSchema = true,
  )
}

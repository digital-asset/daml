// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.testing.postgresql.PostgresAroundAll
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

trait HttpServicePostgresInt extends AbstractHttpServiceIntegrationTestFuns with PostgresAroundAll {
  this: AsyncFreeSpec with Matchers with Inside with StrictLogging =>

  override final def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // has to be lazy because postgresFixture is NOT initialized yet
  protected[this] lazy val jdbcConfig_ = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = postgresDatabase.url,
    user = "test",
    password = "",
    createSchema = true,
  )
}

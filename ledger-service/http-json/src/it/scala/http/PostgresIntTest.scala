// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.dbutils
import com.daml.http.PostgresIntTest.defaultJdbcConfig
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers

class PostgresIntTest
    extends AbstractDatabaseIntegrationTest
    with PostgresAroundAll
    with Matchers
    with Inside {
  override protected def jdbcConfig: JdbcConfig = defaultJdbcConfig(postgresDatabase.url)
}

object PostgresIntTest {
  def defaultJdbcConfig(url: => String) = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = url,
    user = "test",
    password = "",
    dbStartupMode = DbStartupMode.CreateOnly,
    tablePrefix = "some_nice_prefix_",
  )
}

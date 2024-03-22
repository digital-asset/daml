// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServicePostgresInt.defaultJdbcConfig
import dbbackend.JdbcConfig
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

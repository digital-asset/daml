// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import org.scalatest.Suite

trait PostgresAroundSuite extends PostgresAround {
  self: Suite =>

  @volatile
  private var _jdbcUrl: Option[JdbcUrl] = None

  protected def postgresJdbcUrl: JdbcUrl = _jdbcUrl.get

  protected def createNewDatabase(): JdbcUrl = {
    _jdbcUrl = Some(createNewRandomDatabase())
    postgresJdbcUrl
  }
}

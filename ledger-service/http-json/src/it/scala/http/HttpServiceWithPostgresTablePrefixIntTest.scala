// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceWithPostgresTablePrefixIntTest extends HttpServiceWithPostgresIntTest {
  override lazy val jdbcConfig_ = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = postgresDatabase.url,
    user = "test",
    password = "",
    tablePrefix = "some_nice_prefix_",
    dbStartupMode = DbStartupMode.CreateOnly,
  )
}

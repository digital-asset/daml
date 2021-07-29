// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

class HttpServiceWithOracleTablePrefixIntTest
    extends AbstractHttpServiceIntegrationTest
    with HttpServiceOracleInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  override lazy val jdbcConfig_ = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = postgresDatabase.url,
    user = "test",
    password = "",
    tablePrefix = "some_nice_prefix_",
    dbStartupMode = DbStartupMode.CreateOnly,
  )
}

// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll

class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] =
    Some(
      JdbcConfig(
        driver = "org.postgresql.Driver",
        url = postgresFixture.jdbcUrl,
        user = "test",
        password = "",
        createSchema = true))
}

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.slick

class PostgreSqlConnection(jdbcUrl: String, username: String, password: String)
    extends GenericSlickConnection(
      profile = slick.jdbc.PostgresProfile,
      driver = "org.postgresql.Driver",
      url = jdbcUrl,
      user = Some(username),
      password = Some(password)
    )

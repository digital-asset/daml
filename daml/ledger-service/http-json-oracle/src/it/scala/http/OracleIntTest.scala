// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceOracleInt.defaultJdbcConfig
import dbbackend.JdbcConfig
import com.daml.testing.oracle.OracleAroundAll
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers

class OracleIntTest
    extends AbstractDatabaseIntegrationTest
    with OracleAroundAll
    with Matchers
    with Inside {
  override protected def jdbcConfig: JdbcConfig =
    defaultJdbcConfig(oracleJdbcUrlWithoutCredentials, oracleUserName, oracleUserPwd)
}

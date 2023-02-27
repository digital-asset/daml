// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceOracleInt.defaultJdbcConfig
import dbbackend.JdbcConfig
import com.daml.testing.oracle.OracleAroundAll

import akka.stream.scaladsl.Source
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Future

class OracleIntTest
    extends AbstractDatabaseIntegrationTest
    with OracleAroundAll
    with Matchers
    with Inside {
  override protected def jdbcConfig: JdbcConfig =
    defaultJdbcConfig(oracleJdbcUrlWithoutCredentials, oracleUserName, oracleUserPwd)

  "fetchAndPersist" - {
    import dao.{logHandler, jdbcDriver}, jdbcDriver.q.queries

    "avoids class 61 errors under concurrent update" in {
      val fetch = new ContractsFetch(
        (_, _, _, _) => _ => Source.empty,
        (_, _, _, _, _) => _ => Source.empty,
        (_, _) => _ => Future successful None,
      )
      succeed
    }
  }
}

package com.daml.http

import com.daml.http.OracleIntTest.defaultJdbcConfig
import com.daml.testing.oracle.OracleAroundAll
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers

class OracleIntTest
    extends AbstractDatabaseIntegrationTests
    with OracleAroundAll
    with Matchers
    with Inside {
  override protected def jdbcConfig: JdbcConfig =
    defaultJdbcConfig(oracleJdbcUrl, oracleUser, oraclePwd)
}

object OracleIntTest {
  def defaultJdbcConfig(url: => String, user: => String, pwd: => String) = JdbcConfig(
    driver = "oracle.jdbc.OracleDriver",
    url = url,
    user = user,
    password = pwd,
    dbStartupMode = DbStartupMode.CreateOnly,
    tablePrefix = "some_nice_prefix_",
  )
}

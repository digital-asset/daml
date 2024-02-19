// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package http.perf

import dbutils.ConnectionPool
import testing.oracle.OracleAround

import scala.util.{Success, Try}

private[this] final class OracleRunner {

  private val defaultUser = "ORACLE_USER"
  private val retainData = sys.env.get("RETAIN_DATA").exists(_ equalsIgnoreCase "true")
  private val useDefaultUser = sys.env.get("USE_DEFAULT_USER").exists(_ equalsIgnoreCase "true")
  type St = OracleAround.RichOracleUser

  def start(): Try[St] = Try {
    if (useDefaultUser) OracleAround.createOrReuseUser(defaultUser)
    else OracleAround.createNewUniqueRandomUser()
  }

  def jdbcConfig(user: St): JdbcConfig = {
    import DbStartupMode._
    val startupMode: DbStartupMode = if (retainData) CreateIfNeededAndStart else CreateAndStart
    JdbcConfig(
      dbutils.JdbcConfig(
        "oracle.jdbc.OracleDriver",
        user.jdbcUrlWithoutCredentials,
        user.oracleUser.name,
        user.oracleUser.pwd,
        ConnectionPool.PoolSize.Production,
      ),
      startMode = startupMode,
    )
  }

  def stop(user: St): Try[Unit] = {
    if (retainData) Success(()) else Try(user.drop())
  }
}

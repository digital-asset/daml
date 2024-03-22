// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import com.daml.testing.oracle.OracleAround.RichOracleUser
import org.scalatest.Suite

trait OracleAroundSuite {
  self: Suite =>

  @volatile
  private var user: RichOracleUser = _

  def jdbcUrl: String = user.jdbcUrl
  def lockIdSeed: Int = user.lockIdSeed
  def oracleUserName: String = user.oracleUser.name
  def oracleUserPwd: String = user.oracleUser.pwd
  def oracleJdbcUrlWithoutCredentials: String = user.jdbcUrlWithoutCredentials

  protected def createNewUser(): Unit = user = OracleAround.createNewUniqueRandomUser()

  protected def dropUser(): Unit = user.drop()
}

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.oracle

import com.digitalasset.canton.platform.store.testing.oracle.OracleAround.{
  OracleServer,
  RichOracleUser,
}
import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicReference

trait OracleAroundSuite {
  self: Suite =>

  private val user: AtomicReference[RichOracleUser] = new AtomicReference
  private val server: AtomicReference[OracleServer] = new AtomicReference

  def jdbcUrl: String = user.get.jdbcUrl
  def lockIdSeed: Int = user.get.lockIdSeed
  def oracleUserName: String = user.get.oracleUser.name
  def oracleUserPwd: String = user.get.oracleUser.pwd
  def oracleJdbcUrlWithoutCredentials: String = user.get.jdbcUrlWithoutCredentials

  protected def createNewUser(): Unit = user.set(OracleAround.createNewUniqueRandomUser(server.get))
  protected def dropUser(): Unit = user.get.drop()

  protected def connectToServer(): Unit = server.set(OracleAround.connectToOracleServer())
  protected def disconnectFromServer(): Unit = server.get.disconnect()
}

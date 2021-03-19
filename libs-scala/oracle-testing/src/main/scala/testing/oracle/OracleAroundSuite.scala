// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import org.scalatest.Suite

trait OracleAroundSuite extends OracleAround {
  self: Suite =>

  @volatile
  private var user: User = _

  protected def oracleUser: String = user.name
  protected def oraclePwd: String = user.pwd

  protected def createNewUser(): Unit = {
    connectToOracle()
    user = createNewRandomUser()
  }

  protected def dropUser(): Unit = {
    dropUser(oracleUser)
  }
}

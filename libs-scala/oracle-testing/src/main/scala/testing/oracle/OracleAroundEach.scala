// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait OracleAroundEach extends OracleAroundSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    Class.forName("oracle.jdbc.OracleDriver")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    createNewUser()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    dropUser()
  }
}

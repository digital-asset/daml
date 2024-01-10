// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.oracle

import org.scalatest.{BeforeAndAfterAll, Suite}

trait OracleAroundAll extends OracleAroundSuite with BeforeAndAfterAll {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    Class.forName("oracle.jdbc.OracleDriver")
    connectToServer()
    createNewUser()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    dropUser()
    disconnectFromServer()
  }
}

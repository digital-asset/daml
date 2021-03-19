// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import org.scalatest.{BeforeAndAfterAll, Suite}

trait OracleAroundAll extends OracleAroundSuite with BeforeAndAfterAll {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    createNewUser()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    dropUser()
    super.afterAll()
  }
}

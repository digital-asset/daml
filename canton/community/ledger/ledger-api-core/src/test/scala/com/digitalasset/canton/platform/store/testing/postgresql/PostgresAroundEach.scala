// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait PostgresAroundEach
    extends PostgresAroundSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    // We start PostgreSQL before calling `super` because _generally_ the database needs to be up
    // before everything else.
    connectToPostgresqlServer()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    disconnectFromPostgresqlServer()
  }

  override protected def beforeEach(): Unit = {
    // We create the database before calling `super` for the same reasons as above.
    createNewDatabase()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    dropDatabase()
  }
}

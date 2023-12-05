// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import org.scalatest.{BeforeAndAfterAll, Suite}

trait PostgresAroundAll extends PostgresAroundSuite with BeforeAndAfterAll {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    // We start PostgreSQL before calling `super` because _generally_ the database needs to be up
    // before everything else.
    connectToPostgresqlServer()
    createNewDatabase()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    disconnectFromPostgresqlServer()
  }
}

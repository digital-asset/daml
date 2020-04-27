// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import org.scalatest.BeforeAndAfterAll

trait PostgresAroundAll extends PostgresAround with BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  override protected def beforeAll(): Unit = {
    // we start pg before running the rest because _generally_ the database
    // needs to be up before everything else. this is relevant for
    // ScenarioLoadingITPostgres at least. we could much with the mixin
    // order but this was easier...
    startEphemeralPostgres()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopAndCleanUpPostgres()
  }
}

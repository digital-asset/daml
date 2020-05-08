// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import org.scalatest.Suite

trait PostgresAroundSuite extends PostgresAround {
  self: Suite =>

  @volatile
  private var database: Option[PostgresDatabase] = None

  protected def postgresDatabase: PostgresDatabase = database.get

  protected def createNewDatabase(): PostgresDatabase = {
    database = Some(createNewRandomDatabase())
    postgresDatabase
  }

  protected def dropDatabase(): Unit = {
    dropDatabase(postgresDatabase)
    database = None
  }
}

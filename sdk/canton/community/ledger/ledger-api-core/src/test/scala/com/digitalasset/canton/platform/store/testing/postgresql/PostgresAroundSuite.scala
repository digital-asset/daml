// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import org.scalatest.Suite

trait PostgresAroundSuite extends PostgresAround {
  self: Suite =>

  @volatile
  private var database: Option[PostgresDatabase] = None

  protected def jdbcUrl: String = postgresDatabase.url

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  protected def postgresDatabase: PostgresDatabase = database.get

  protected def lockIdSeed: Int =
    1000 // For postgres each test-suite uses different DB, so no unique lock-ids needed

  protected def createNewDatabase(): PostgresDatabase = {
    database = Some(createNewRandomDatabase())
    postgresDatabase
  }

  protected def dropDatabase(): Unit = {
    dropDatabase(postgresDatabase)
    database = None
  }
}

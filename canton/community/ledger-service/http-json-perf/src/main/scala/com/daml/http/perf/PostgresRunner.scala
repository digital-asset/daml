// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import com.daml.testing.postgresql.{PostgresAroundSuite, PostgresDatabase}
import org.scalatest.Suite

import scala.util.Try

class PostgresRunner {

  private val suite = new PostgresRunnerSuite()

  def start(): Try[PostgresDatabase] = suite.start()

  def stop(): Try[Unit] = suite.stop()
}

private class PostgresRunnerSuite extends PostgresAroundSuite with Suite {
  def start(): Try[PostgresDatabase] =
    for {
      _ <- Try(this.connectToPostgresqlServer())
      db <- Try(this.createNewDatabase())
    } yield db

  def stop(): Try[Unit] = Try(this.disconnectFromPostgresqlServer())
}

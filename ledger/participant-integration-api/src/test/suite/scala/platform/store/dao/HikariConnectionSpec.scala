// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.resources.TestResourceContext
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.DbType
import com.daml.testing.postgresql.PostgresAroundAll
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class HikariConnectionSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with TestResourceContext
    with PostgresAroundAll {

  it should "use asynchronous commits if enabled" in {
    assertAsyncCommit(asyncCommitEnabled = true, expectedQueryResponse = "off")
  }

  it should "not use asynchronous commits if disabled" in {
    assertAsyncCommit(asyncCommitEnabled = false, expectedQueryResponse = "on")
  }

  private def assertAsyncCommit(asyncCommitEnabled: Boolean, expectedQueryResponse: String) =
    newHikariConnection(asyncCommitEnabled)
      .use { hikariDataSource =>
        val rs =
          hikariDataSource.getConnection.createStatement().executeQuery("SHOW synchronous_commit")
        rs.next()
        rs.getString(1) shouldBe expectedQueryResponse
      }

  private def newHikariConnection(asyncCommitEnabled: Boolean) =
    HikariConnection.owner(
      serverRole = ServerRole.Testing(this.getClass),
      jdbcUrl = postgresDatabase.url,
      minimumIdle = 2,
      maxPoolSize = 2,
      connectionTimeout = 5.seconds,
      metrics = None,
      connectionAsyncCommitMode =
        if (asyncCommitEnabled) DbType.AsynchronousCommit else DbType.SynchronousCommit,
    )(LoggingContext.ForTesting)
}

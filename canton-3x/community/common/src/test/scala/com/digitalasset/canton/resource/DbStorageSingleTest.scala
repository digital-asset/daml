// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.config.CommunityDbConfig.Postgres
import com.digitalasset.canton.config.{CommunityDbConfig, DbConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.store.db.DbStorageSetup
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, CloseableTest}
import org.scalatest.wordspec.AsyncWordSpec

trait DbStorageSingleTest extends AsyncWordSpec with BaseTest with CloseableTest {

  def baseConfig: DbConfig
  def modifyUser(user: String): DbConfig
  def modifyPassword(password: String): DbConfig
  def modifyPort(port: Int): DbConfig
  def modifyDatabaseName(dbName: String): DbConfig
  val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)

  "DbStorage" should {

    "config should not leak confidential data" in {
      val stinky = "VERYSTINKYPASSWORD"
      (modifyPassword(stinky).toString) should not include (stinky)
    }

    "connect on correct config" in {
      val config = baseConfig
      DbStorageSingle
        .create(
          config,
          connectionPoolForParticipant = false,
          None,
          clock,
          None,
          CommonMockMetrics.dbStorage,
          DefaultProcessingTimeouts.testing,
          loggerFactory,
        )
        .valueOrFailShutdown("storage create") shouldBe a[DbStorageSingle]
    }

    "fail on invalid credentials" in {
      val config = modifyUser("foobar")
      loggerFactory.suppressWarningsAndErrors {
        DbStorageSingle
          .create(
            config,
            connectionPoolForParticipant = false,
            None,
            clock,
            None,
            CommonMockMetrics.dbStorage,
            DefaultProcessingTimeouts.testing,
            loggerFactory,
          )
          .leftOrFailShutdown("storage create") shouldBe a[String]
      }
    }

    "fail on invalid database" in {
      val config = modifyDatabaseName("foobar")
      loggerFactory.suppressWarningsAndErrors {
        DbStorageSingle
          .create(
            config,
            connectionPoolForParticipant = false,
            None,
            clock,
            None,
            CommonMockMetrics.dbStorage,
            DefaultProcessingTimeouts.testing,
            loggerFactory,
          )
          .leftOrFailShutdown("storage create") shouldBe a[String]
      }
    }

    "fail on invalid port" in {
      val config = modifyPort(14001)
      loggerFactory.suppressWarningsAndErrors {
        DbStorageSingle
          .create(
            config,
            connectionPoolForParticipant = false,
            None,
            clock,
            None,
            CommonMockMetrics.dbStorage,
            DefaultProcessingTimeouts.testing,
            loggerFactory,
          )
          .leftOrFailShutdown("storage create") shouldBe a[String]
      }
    }
  }

}

class DbStorageSingleTestPostgres extends DbStorageSingleTest {

  private lazy val setup = DbStorageSetup.postgres(loggerFactory)

  private def modifyConfig(config: DbBasicConfig): Postgres =
    CommunityDbConfig.Postgres(config.toPostgresConfig)

  def baseConfig: Postgres = modifyConfig(setup.basicConfig)

  def modifyUser(userName: String): Postgres =
    modifyConfig(setup.basicConfig.copy(username = userName))

  def modifyPassword(password: String): Postgres =
    modifyConfig(setup.basicConfig.copy(password = password))

  def modifyPort(port: Int): Postgres =
    modifyConfig(setup.basicConfig.copy(port = port))

  def modifyDatabaseName(dbName: String): Postgres =
    modifyConfig(setup.basicConfig.copy(dbName = dbName))
}

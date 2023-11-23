// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CommunityDbConfig.*
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

class DbConfigTest extends AnyWordSpec with BaseTest {
  def pgConfig(url: String = "jdbc:postgresql://0.0.0.0:0/dbName") =
    Postgres(ConfigFactory.parseString(s"""
                                          |{
                                          |  url = "${url}"
                                          |  user = "user"
                                          |  password = "pass"
                                          |  driver = org.postgresql.Driver
                                          |}
    """.stripMargin))

  lazy val h2ConfigWithoutRequiredOptions =
    H2(ConfigFactory.parseString(s"""
                                    |{
                                    |  url = "jdbc:h2:mem:dbName"
                                    |  user = "user"
                                    |  password = "pass"
                                    |}
    """.stripMargin))

  lazy val h2ConfigWithRequiredOptions =
    H2(ConfigFactory.parseString(s"""
                                    |{
                                    |  url = "jdbc:h2:mem:dbName;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
                                    |  user = "user"
                                    |  password = "pass"
                                    |}
    """.stripMargin))

  lazy val numCores = Threading.detectNumberOfThreads(noTracingLogger)
  lazy val numThreads = Math.max(1, numCores / 2)
  lazy val connTimeout = DbConfig.defaultConnectionTimeout.unwrap.toMillis

  def pgExpectedConfig(expectedNumThreads: Int): Config =
    ConfigFactory.parseString(s"""{
                                 |  driver = org.postgresql.Driver
                                 |  numThreads = $expectedNumThreads
                                 |  password = "pass"
                                 |  poolName = "poolName"
                                 |  url = "jdbc:postgresql://0.0.0.0:0/dbName"
                                 |  user = "user"
                                 |  connectionTimeout = $connTimeout
                                 |  initializationFailTimeout = 1
                                 |}
    """.stripMargin)

  "DbConfig.configWithFallback" should {
    val config = pgConfig()
    "Add default values to postgres config" in {
      DbConfig
        .configWithFallback(config)(
          config.numReadConnectionsCanton(forParticipant = true),
          "poolName",
          logger,
        ) shouldBe pgExpectedConfig(Math.max(1, numThreads / 2))
    }

    "Adjust the number of threads for replicated participants" in {
      DbConfig.configWithFallback(config)(
        config.numReadConnectionsCanton(forParticipant = true),
        "poolName",
        logger,
      ) shouldBe pgExpectedConfig(Math.max(1, numThreads / 2))
    }

    "Adjust the number of threads for replicated participants write connection pool" in {
      DbConfig.configWithFallback(config)(
        config.numWriteConnectionsCanton(forParticipant = true),
        "poolName",
        logger,
      ) shouldBe pgExpectedConfig(Math.max(1, numThreads / 2 - 1))
    }

    "Add default values to H2 config and enforced options" in {
      DbConfig.configWithFallback(h2ConfigWithRequiredOptions)(
        config.numReadConnectionsCanton(forParticipant = true),
        "poolName",
        logger,
      ) shouldBe
        ConfigFactory.parseString(s"""{
                                             |  driver = org.h2.Driver
                                             |  numThreads = 1
                                             |  password = "pass"
                                             |  poolName = "poolName"
                                             |  url = "jdbc:h2:mem:dbName;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
                                             |  user = "user"
                                             |  connectionTimeout = $connTimeout
                                             |  initializationFailTimeout = 1
                                             |}
    """.stripMargin)

      loggerFactory.suppressWarningsAndErrors(
        DbConfig.configWithFallback(h2ConfigWithoutRequiredOptions)(
          config.numReadConnectionsCanton(forParticipant = true),
          "poolName",
          logger,
        ) shouldBe
          ConfigFactory.parseString(s"""{
                                       |  driver = org.h2.Driver
                                       |  numThreads = 1
                                       |  password = "pass"
                                       |  poolName = "poolName"
                                       |  properties:{DB_CLOSE_DELAY:"-1",MODE:"PostgreSQL"}
                                       |  url = "jdbc:h2:mem:dbName"
                                       |  user = "user"
                                       |  connectionTimeout = $connTimeout
                                       |  initializationFailTimeout = 1
                                       |}
    """.stripMargin)
      )
    }
  }
}

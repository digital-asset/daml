// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.dbutils
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}

final class CliSpec extends AnyFreeSpec with Matchers {

  private def configParser(
      parameters: Seq[String],
      getEnvVar: String => Option[String] = (_ => None),
  ): Option[Config] =
    Cli.parseConfig(parameters, Set("org.postgresql.Driver"), getEnvVar)

  val jdbcConfig = JdbcConfig(
    dbutils.JdbcConfig(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/test?&ssl=true",
      "postgres",
      "password",
      poolSize = 10,
      minIdle = 4,
      connectionTimeout = 5000,
      idleTimeout = 1000,
      tablePrefix = "foo",
    ),
    dbStartupMode = DbStartupMode.StartOnly,
  )
  val jdbcConfigString =
    "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password," +
      "poolSize=10,minIdle=4,connectionTimeout=5000,idleTimeout=1000,tablePrefix=foo"

  val sharedOptions =
    Seq("--ledger-host", "localhost", "--ledger-port", "6865", "--http-port", "7500")

  "LogLevel" - {
    import ch.qos.logback.classic.{Level => LogLevel}

    def logLevelArgs(level: String) = Seq("--log-level", level)

    def checkLogLevelWorks(level: String, expected: LogLevel) = {
      val config = configParser(logLevelArgs(level) ++ sharedOptions)
        .getOrElse(fail())
      config.logLevel shouldBe Some(expected)
    }

    "should get the error log level from the command line argument when provided" in {
      checkLogLevelWorks("error", LogLevel.ERROR)
    }

    "should get the warn log level from the command line argument when provided" in {
      checkLogLevelWorks("warn", LogLevel.WARN)
    }

    "should get the info log level from the command line argument when provided" in {
      checkLogLevelWorks("info", LogLevel.INFO)
    }

    "should get the debug log level from the command line argument when provided" in {
      checkLogLevelWorks("debug", LogLevel.DEBUG)
    }

    "should get the trace log level from the command line argument when provided" in {
      checkLogLevelWorks("trace", LogLevel.TRACE)
    }

    "shouldn't get a config parser result if an invalid log level is provided via a command line argument" in {
      val config = configParser(logLevelArgs("SUPERFANCYLOGLEVEL") ++ sharedOptions)
      config shouldBe None
    }

    "should get a config parser result if no log level is provided via a command line argument" in {
      val config = configParser(sharedOptions).getOrElse(fail())
      config.logLevel shouldBe None
    }
  }

  "JdbcConfig" - {
    "should get the jdbc string from the command line argument when provided" in {
      val config = configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
        .getOrElse(fail())
      config.jdbcConfig shouldBe Some(jdbcConfig)
    }

    "should get the jdbc string from the environment when provided" in {
      val jdbcEnvVar = "JDBC_ENV_VAR"
      val config = configParser(
        Seq("--query-store-jdbc-config-env", jdbcEnvVar) ++ sharedOptions,
        { case `jdbcEnvVar` => Some(jdbcConfigString) },
      ).getOrElse(fail())
      config.jdbcConfig shouldBe Some(jdbcConfig)
    }

    "should get None when neither CLI nor env variable are provided" in {
      val config = configParser(sharedOptions).getOrElse(fail())
      config.jdbcConfig shouldBe None
    }

    "should get the table prefix if specified" in {
      val prefix = "some_fancy_prefix_"
      val config = configParser(
        Seq("--query-store-jdbc-config", s"$jdbcConfigString,tablePrefix=$prefix") ++ sharedOptions
      ).getOrElse(fail())
      config.jdbcConfig shouldBe Some(
        jdbcConfig.copy(baseConfig = jdbcConfig.baseConfig.copy(tablePrefix = prefix))
      )
    }

    "DbStartupMode" - {
      val jdbcConfigShared =
        "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres," +
          "password=password,poolSize=10,minIdle=4,connectionTimeout=5000,idleTimeout=1000,tablePrefix=foo"

      "should get the CreateOnly startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-only"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(jdbcConfig.copy(dbStartupMode = DbStartupMode.CreateOnly))
      }

      "should get the StartOnly startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=start-only"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(jdbcConfig.copy(dbStartupMode = DbStartupMode.StartOnly))
      }

      "should get the CreateIfNeededAndStart startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-if-needed-and-start"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(dbStartupMode = DbStartupMode.CreateIfNeededAndStart)
        )
      }

      "should get the CreateAndStart startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-and-start"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(dbStartupMode = DbStartupMode.CreateAndStart)
        )
      }

      "createSchema=false is converted to StartOnly" in {
        val jdbcConfigString = s"$jdbcConfigShared,createSchema=false"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(dbStartupMode = DbStartupMode.StartOnly)
        )
      }

      "createSchema=true is converted to CreateOnly" in {
        val jdbcConfigString = s"$jdbcConfigShared,createSchema=true"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(dbStartupMode = DbStartupMode.CreateOnly)
        )
      }
    }

    "DisableContractPayloadIndexing" - {
      import dbbackend.Queries.Oracle
      import dbbackend.OracleQueries.DisableContractPayloadIndexing
      // we can always use postgres here; but if we integrate driver init with cmdline parse
      // we'll have to restrict this test block to EE-only
      val jdbcConfigShared =
        "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password"
      val expectedExtraConf = Map(DisableContractPayloadIndexing -> "true")

      "parses from command-line string" in {
        val jdbcConfigString = s"$jdbcConfigShared,$DisableContractPayloadIndexing=true"
        val config = configParser(
          Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions
        ).getOrElse(fail())
        import org.scalatest.OptionValues._
        config.jdbcConfig.value.backendSpecificConf should ===(expectedExtraConf)
      }

      "then parses through backend" in {
        Oracle.parseConf(expectedExtraConf) should ===(Right(true: DisableContractPayloadIndexing))
      }

      "or defaults to false" in {
        Oracle.parseConf(Map.empty) should ===(Right(false: DisableContractPayloadIndexing))
      }

      "but fails on bad input" in {
        Oracle.parseConf(Map(DisableContractPayloadIndexing -> "haha")).isLeft should ===(true)
      }
    }
  }

}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

final class CliSpec extends AnyFreeSpec with Matchers {

  private def configParser(
      parameters: Seq[String],
      getEnvVar: String => Option[String] = (_ => None),
  ): Option[Config] =
    Cli.parseConfig(parameters, getEnvVar)

  val jdbcConfig = JdbcConfig(
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/test?&ssl=true",
    "postgres",
    "password",
    poolSize = 10,
    minIdle = 4,
    connectionTimeout = 5000,
    idleTimeout = 1000,
  )
  val jdbcConfigString =
    "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password," +
      "createSchema=false,poolSize=10,minIdle=4,connectionTimeout=5000,idleTimeout=1000"

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
  }

}

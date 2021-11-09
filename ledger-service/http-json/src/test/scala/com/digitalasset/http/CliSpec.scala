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
  )
  val jdbcConfigString =
    "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false,poolSize=10"

  val sharedOptions =
    Seq("--ledger-host", "localhost", "--ledger-port", "6865", "--http-port", "7500")

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

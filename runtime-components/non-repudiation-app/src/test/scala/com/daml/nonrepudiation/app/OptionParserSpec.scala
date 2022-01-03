// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.app

import java.net.InetSocketAddress

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class OptionParserSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "OptionParser"

  it should "parse the expected options" in {

    val options = Array(
      "--ledger-host",
      "168.10.67.4",
      "--ledger-port",
      "7890",
      "--proxy-host",
      "168.10.67.5",
      "--proxy-port",
      "7891",
      "--api-host",
      "168.10.67.6",
      "--api-port",
      "7892",
      "--jdbc",
      "url=jdbc:postgresql:nr,user=psql,password=secret",
      "--database-max-pool-size",
      "30",
    )

    val expectedConfiguration =
      Configuration(
        participantAddress = new InetSocketAddress("168.10.67.4", 7890),
        proxyAddress = new InetSocketAddress("168.10.67.5", 7891),
        apiAddress = new InetSocketAddress("168.10.67.6", 7892),
        apiShutdownTimeout = Configuration.Default.apiShutdownTimeout,
        databaseJdbcUrl = "jdbc:postgresql:nr",
        databaseJdbcUsername = "psql",
        databaseJdbcPassword = "secret",
        databaseMaxPoolSize = 30,
        metricsReportingPeriod = Configuration.Default.metricsReportingPeriod,
      )

    OptionParser.parse(options, Configuration.Default).value shouldBe expectedConfiguration

  }

}

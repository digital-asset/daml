// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.config

import java.io.File

import com.daml.ledger.client.binding.LedgerClientConfigurationError.MalformedTypesafeConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class LedgerClientConfigTest extends AnyWordSpec with Matchers {

  "TypeSafePlatformConfig" should {

    "parse the reference conf without errors" in {
      LedgerClientConfig.create() should be(a[Success[_]])
    }

    "parse the expected values out of the reference conf" in {
      val config = LedgerClientConfig.create().get

      config.ledgerId shouldEqual None
      config.commandClient.maxCommandsInFlight shouldEqual 256
      config.commandClient.maxParallelSubmissions shouldEqual 32
      config.commandClient.defaultDeduplicationTime.getSeconds shouldEqual 30
      config.maxRetryTime.getSeconds shouldEqual 60
      config.ssl shouldBe None
    }

    "parse the expected values out of the mock config" in {
      val configStr = """
                        |ledger-client {
                        |  ledger-id = "ledgerId_mock"
                        |  command-client {
                        |    max-commands-in-flight = 260
                        |    max-parallel-submissions = 40
                        |    default-deduplication-time = PT40S
                        |  }
                        |  max-retry-time = PT45S
                        |  ssl {
                        |    client-key-cert-chain-file = "file1"
                        |    client-key-file = "file2"
                        |    trusted-certs-file = "file3"
                        |  }
                        |}""".stripMargin

      val clientConfig = LedgerClientConfig.create(ConfigFactory.parseString(configStr)).get

      clientConfig.ledgerId shouldEqual Some("ledgerId_mock")
      clientConfig.commandClient.maxCommandsInFlight shouldEqual 260
      clientConfig.commandClient.maxParallelSubmissions shouldEqual 40
      clientConfig.commandClient.defaultDeduplicationTime.getSeconds shouldEqual 40
      clientConfig.maxRetryTime.getSeconds shouldEqual 45
      clientConfig.ssl.get.clientKeyCertChainFile shouldBe new File("file1")
      clientConfig.ssl.get.clientKeyFile shouldBe new File("file2")
      clientConfig.ssl.get.trustedCertsFile shouldBe new File("file3")
    }

    "return the expected type of Throwable on parse errors" in {
      LedgerClientConfig.create(ConfigFactory.empty()).failed.get should be(
        a[MalformedTypesafeConfig])
    }
  }
}

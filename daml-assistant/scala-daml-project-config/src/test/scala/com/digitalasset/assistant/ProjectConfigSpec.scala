// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.assistant.config

import org.scalatest.{Matchers, WordSpec}

class ProjectConfigSpec extends WordSpec with Matchers {
  "ProjectConfig" when {

    "Loading a default config" should {
      val configSource =
        """
          |sdk-version: "1.0"
          |name: TestProject
          |source: daml/Main.daml
          |scenario: Main:setup
          |parties:
          |  - Alice
          |  - Bob
          |version: 1.0.0
          |exposed-modules:
          |  - Main
          |dependencies:
          |  - daml-prim
          |  - daml-stdlib
        """.stripMargin

      "find the SDK version" in {
        val sdkVersion = for {
          config <- ProjectConfig.loadFromString(configSource)
          result <- config.sdkVersion
        } yield result
        sdkVersion shouldBe Right("1.0")
      }

      "find the name" in {
        val name = for {
          config <- ProjectConfig.loadFromString(configSource)
          result <- config.name
        } yield result
        name shouldBe Right(Some("TestProject"))
      }

      "find the parties" in {
        val parties = for {
          config <- ProjectConfig.loadFromString(configSource)
          result <- config.parties
        } yield result
        parties shouldBe Right(Some(List("Alice", "Bob")))
      }

      "empty ledger config" in {
        val ledger = for {
          config <- ProjectConfig.loadFromString(configSource)
          result <- config.ledger
        } yield result
        ledger shouldBe Right(None)
      }
    }

    "Loading a minimal config" should {
      val config =
        """
          |sdk-version: "1.0"
        """.stripMargin

      "not find the name" in {
        val name = for {
          config <- ProjectConfig.loadFromString(config)
          result <- config.name
        } yield result
        name shouldBe Right(None)
      }
    }

    "Loading a config with broken properties" should {
      val config =
        """
          |sdk-version: "1.0"
          |parties: Alice
        """.stripMargin

      "not find the name" in {
        val parties = for {
          config <- ProjectConfig.loadFromString(config)
          result <- config.parties
        } yield result
        parties.isLeft shouldBe true
      }
    }

    "Loading a broken config" should {
      val configSource =
        """
          |:
        """.stripMargin

      "fail to parse" in {
        val config = for {
          result <- ProjectConfig.loadFromString(configSource)
        } yield result
        config.isLeft shouldBe true
        config.left.exists {
          case pe: ConfigParseError => true
          case _ => false
        } shouldBe true
      }
    }

    "Loading a ledger config" should {
      "find the config" in {
        val config =
          """
            |ledger:
            |  port: 12345
            |  address: host.domain.com
            |  wall-clock-time: true
            |  max-inbound-message-size-bytes: 98765
            |  ledger-id: ledger-id-string-1234567890
            |  max-ttl-seconds: 17
        """.stripMargin

        val ledgerConfig = for {
          config <- ProjectConfig.loadFromString(config)
          result <- config.ledger
        } yield result

        ledgerConfig shouldBe Right(
          Some(LedgerConfig(
            port = Some(12345),
            address = Some("host.domain.com"),
            wallClockTime = Some(true),
            maxInboundMessageSize = Some(98765),
            ledgerId = Some("ledger-id-string-1234567890"),
            maxTtlSeconds = Some(17)
          )))
      }

      "not return an empty config" in {
        val config =
          """
            |ledger:
        """.stripMargin

        val ledgerConfig = for {
          config <- ProjectConfig.loadFromString(config)
          result <- config.ledger
        } yield result

        ledgerConfig shouldBe Right(None)
      }
    }
  }
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.assistant.config

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProjectConfigSpec extends AnyWordSpec with Matchers {
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
          case _: ConfigParseError => true
          case _ => false
        } shouldBe true
      }
    }
  }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.assistant.config

import java.nio.file.Paths

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProjectConfigSpec extends AnyWordSpec with Matchers {

  private val projectRoot = Paths.get("/project/root")

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
          config <- ProjectConfig.loadFromString(projectRoot, configSource)
          result <- config.sdkVersion
        } yield result
        sdkVersion shouldBe Right("1.0")
      }

      "find the name" in {
        val name = for {
          config <- ProjectConfig.loadFromString(projectRoot, configSource)
          result <- config.name
        } yield result
        name shouldBe Right(Some("TestProject"))
      }

      "find the parties" in {
        val parties = for {
          config <- ProjectConfig.loadFromString(projectRoot, configSource)
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
          config <- ProjectConfig.loadFromString(projectRoot, config)
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
          config <- ProjectConfig.loadFromString(projectRoot, config)
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
          result <- ProjectConfig.loadFromString(projectRoot, configSource)
        } yield result
        config.isLeft shouldBe true
        config.left.exists {
          case _: ConfigParseError => true
          case _ => false
        } shouldBe true
      }
    }

    "Loading a config with environment variable interpolation" should {
      "replace valid variable" in {
        val version = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            "version: ${MY_VERSION}",
            Map(("MY_VERSION", "0.0.0")),
          )
          result <- config.version
        } yield result
        version shouldBe Right(Some("0.0.0"))
      }
      "replace value with multiple variables" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            "name: ${PACKAGE_TYPE}-project-${PACKAGE_VISIBILITY}",
            Map(("PACKAGE_TYPE", "production"), ("PACKAGE_VISIBILITY", "public")),
          )
          result <- config.name
        } yield result
        name shouldBe Right(Some("production-project-public"))
      }
      "not replace escaped variable" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            """name: \${MY_NAME}""",
            Map(("MY_NAME", "name")),
          )
          result <- config.name
        } yield result
        name shouldBe Right(Some("${MY_NAME}"))
      }
      "replace double escaped variable" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            """name: \\${MY_NAME}""",
            Map(("MY_NAME", "name")),
          )
          result <- config.name
        } yield result
        name shouldBe Right(Some("""\name"""))
      }
      "not add syntax/structure" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            "name: ${MY_NAME}",
            Map(("MY_NAME", "\n  - elem\n  - elem")),
          )
          result <- config.name
        } yield result
        name shouldBe Right(Some("\n  - elem\n  - elem"))
      }
      "fail when variable doesn't exist" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(projectRoot, "name: ${MY_NAME}", Map())
          result <- config.name
        } yield result
        name shouldBe Left(
          ConfigParseError("""Couldn't find environment variable MY_NAME in value ${MY_NAME}""")
        )
      }
      "not interpolate when feature is disabled via field" in {
        val name = for {
          config <- ProjectConfig.loadFromStringWithEnv(
            projectRoot,
            "name: ${MY_NAME}\nenvironment-variable-interpolation: false",
            Map(),
          )
          result <- config.name
        } yield result
        name shouldBe Right(Some("${MY_NAME}"))
      }
    }
  }
}

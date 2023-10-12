// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DeprecatedConfigUtils.*
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level.INFO
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.*

import scala.annotation.nowarn

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class DeprecatedConfigUtilsTest extends AnyWordSpec with BaseTest {
  case class TestConfig(s: Option[String], i: Option[Int], nested: NestedTestConfig)
  case class NestedTestConfig(newS: String = "bye", newI: Int = 31, newJ: Int = 34)

  private implicit val deprecations: DeprecatedFieldsFor[TestConfig] =
    new DeprecatedFieldsFor[TestConfig] {
      override val movedFields: List[MovedConfigPath] = List(
        MovedConfigPath("i", "nested.new-i", "nested.new-j"),
        MovedConfigPath("s", "nested.new-s"),
      )
    }

  implicit val nestedTestConfigReader: ConfigReader[NestedTestConfig] =
    deriveReader[NestedTestConfig]
  implicit val testConfigReader: ConfigReader[TestConfig] =
    deriveReader[TestConfig].applyDeprecations

  private val expectedLogs = LogEntry.assertLogSeq(
    Seq(
      (
        _.message shouldBe "Config field at s is deprecated. Please use the following path(s) instead: nested.new-s.",
        "deprecated field not logged",
      ),
      (
        _.message shouldBe "Config field at i is deprecated. Please use the following path(s) instead: nested.new-i, nested.new-j.",
        "deprecated field not logged",
      ),
    ),
    Seq.empty,
  ) _

  "DeprecatedConfigUtils" should {
    "use deprecated values as fallback" in {
      val config = ConfigFactory.parseString("""
                                               |{
                                               |  s = "hello"
                                               |  i = 5
                                               |}
                                               |""".stripMargin)

      loggerFactory.assertLogsSeq(SuppressionRule.Level(INFO))(
        {
          val testConfig = pureconfig.ConfigSource
            .fromConfig(config)
            .loadOrThrow[TestConfig]

          testConfig.s shouldBe None
          testConfig.i shouldBe None
          testConfig.nested.newS shouldBe "hello" // Uses "hello", despite newS having a default value, because newS was not set
          testConfig.nested.newI shouldBe 5 // Uses 5 as default value for newI because it was not set
          testConfig.nested.newJ shouldBe 5 // Uses 5 as default value for newI2 because it was not set
        },
        expectedLogs,
      )
    }

    "not overwrite new values" in {
      val config = ConfigFactory.parseString("""
                                               |{
                                               |  s = "hello"
                                               |  i = 5
                                               |  nested {
                                               |    new-i = 10
                                               |    new-j = 11
                                               |    new-s = "bonjour"
                                               |  }
                                               |}
                                               |""".stripMargin)

      loggerFactory.assertLogsSeq(SuppressionRule.Level(INFO))(
        {
          val testConfig = pureconfig.ConfigSource
            .fromConfig(config)
            .loadOrThrow[TestConfig]

          testConfig.s shouldBe None
          testConfig.i shouldBe None
          testConfig.nested.newS shouldBe "bonjour" // Uses "bonjour" because newS was set
          testConfig.nested.newI shouldBe 10 // Uses 10 because newI was set
          testConfig.nested.newJ shouldBe 11 // Uses 11 because newI was set
        },
        expectedLogs,
      )
    }

    "use new default values if nothing is set" in {
      val config = ConfigFactory.parseString("""
                                               |{
                                               | nested {}
                                               |}
                                               |""".stripMargin)

      val testConfig = pureconfig.ConfigSource
        .fromConfig(config)
        .loadOrThrow[TestConfig]

      testConfig.s shouldBe empty
      testConfig.i shouldBe empty
      testConfig.nested.newS shouldBe "bye" // Uses "bye" because nothing is set and it's the default value
      testConfig.nested.newI shouldBe 31 // Uses 31 because nothing is set and it's the default value
      testConfig.nested.newJ shouldBe 34 // Uses 34 because nothing is set and it's the default value
    }
  }
}

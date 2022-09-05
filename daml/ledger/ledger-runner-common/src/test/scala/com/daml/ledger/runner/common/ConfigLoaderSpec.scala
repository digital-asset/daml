// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.runner.common.ConfigLoaderSpec.{ComplexObject, InnerObject, TestScope}
import com.typesafe.config.{Config => TypesafeConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto._

import java.io.File
import java.nio.file.Paths

class ConfigLoaderSpec extends AnyFlatSpec with Matchers with EitherValues {
  behavior of "ConfigLoader.toTypesafeConfig"

  it should "load defaults if no file or key-value map is provided" in new TestScope {
    ConfigLoader.toTypesafeConfig(fallback = empty) shouldBe ConfigFactory.empty()
  }

  it should "override value from the configMap" in new TestScope {
    ConfigLoader.toTypesafeConfig(
      configMap = Map("a" -> "c"),
      fallback = updatedConfig(empty, "a", "b"),
    ) shouldBe updatedConfig(empty, "a", "c")
  }

  it should "load correctly config file" in new TestScope {
    val testConf = ConfigLoader.toTypesafeConfig(
      configFiles = Seq(testFileConfig),
      fallback = empty,
    )
    testConf.getString("test.value-1") shouldBe "v1"
    testConf.getString("test.value-2") shouldBe "v2"
    testConf.getString("test.value-3") shouldBe "v3"
  }

  it should "override one config file by another" in new TestScope {
    val testConf = ConfigLoader.toTypesafeConfig(
      configFiles = Seq(testFileConfig, testFileConfig2),
      fallback = empty,
    )
    testConf.getString("test.value-1") shouldBe "overriden_v1"
    testConf.getString("test.value-2") shouldBe "overriden_v2"
    testConf.getString("test.value-3") shouldBe "v3"
  }

  it should "take precedence of configMap over configFiles" in new TestScope {
    val testConf = ConfigLoader.toTypesafeConfig(
      configFiles = Seq(testFileConfig, testFileConfig2),
      configMap = Map("test.value-1" -> "configmapvalue"),
      fallback = empty,
    )
    testConf.getString("test.value-1") shouldBe "configmapvalue"
    testConf.getString("test.value-2") shouldBe "overriden_v2"
    testConf.getString("test.value-3") shouldBe "v3"
  }

  it should "overwrite complex objects with simple value" in new TestScope {
    val testConf = ConfigLoader.toTypesafeConfig(
      configFiles = Seq(testFileConfig),
      configMap = Map("test" -> "configmapvalue"),
      fallback = empty,
    )
    testConf.getString("test") shouldBe "configmapvalue"
    testConf.hasPath("test.value-1") shouldBe false
    testConf.hasPath("test.value-2") shouldBe false
    testConf.hasPath("test.value-3") shouldBe false
  }

  behavior of "ConfigLoader.loadConfig"

  it should "fail to load empty config" in new TestScope {
    val actualValue: String = ConfigLoader
      .loadConfig[ComplexObject](ConfigFactory.empty())
      .left
      .value
    actualValue shouldBe
      """Failed to load configuration: 
        |at the root:
        |  - (empty config) Key not found: 'value-1'.
        |  - (empty config) Key not found: 'value-2'.""".stripMargin
  }

  it should "successfully load config" in new TestScope {
    import ComplexObject._

    ConfigLoader.loadConfig[ComplexObject](fileConfig).value shouldBe ComplexObject(
      value1 = "v1",
      value2 = "v2",
      value3 = "v3",
    )
  }

  it should "successfully load config and take default from the object if value not provided" in new TestScope {
    import ComplexObject._

    val testConf = ConfigLoader.toTypesafeConfig(
      configMap = Map("value-1" -> "v1", "value-2" -> "v2"),
      fallback = empty,
    )

    val actual: Either[String, ComplexObject] = ConfigLoader.loadConfig[ComplexObject](testConf)
    actual.value shouldBe ComplexObject(
      value1 = "v1",
      value2 = "v2",
      value3 = "default_value",
    )
  }

  it should "support complex objects override with empty string" in new TestScope {
    ConfigLoader
      .loadConfig[ComplexObject](
        ConfigLoader.toTypesafeConfig(
          fallback = complexConfig
        )
      )
      .value shouldBe ComplexObject(
      value1 = "v1",
      value2 = "v2",
      value3 = "v3",
      inner = Some(InnerObject("inner_v1", "inner_v2")),
    )

    ConfigLoader
      .loadConfig[ComplexObject](
        ConfigLoader.toTypesafeConfig(
          fallback = complexConfig,
          configMap = Map("inner" -> ""),
        )
      )
      .value shouldBe ComplexObject(
      value1 = "v1",
      value2 = "v2",
      value3 = "v3",
      inner = None,
    )
  }

  behavior of "ConfigLoader.loadConfig for com.daml.ledger.runner.common.Config"

  it should "load config from empty config and resolve to default" in {
    import PureConfigReaderWriter.Secure._
    val fromConfig = ConfigLoader.loadConfig[Config](ConfigFactory.empty())
    fromConfig.value shouldBe Config.Default
  }

  it should "load default from the default constructor" in {
    import PureConfigReaderWriter.Secure._
    ConfigFactory.invalidateCaches()
    val fromConfig = ConfigLoader.loadConfig[Config](ConfigFactory.load())
    fromConfig.value shouldBe Config.Default
  }

}

object ConfigLoaderSpec {
  case class InnerObject(value1: String, value2: String)
  object InnerObject {
    implicit val Convert: ConfigConvert[InnerObject] = deriveConvert[InnerObject]
  }
  case class ComplexObject(
      value1: String,
      value2: String,
      value3: String = "default_value",
      inner: Option[InnerObject] = None,
  )
  object ComplexObject {
    implicit val Convert: ConfigConvert[ComplexObject] = deriveConvert[ComplexObject]
  }
  trait TestScope {
    val complexConfig = ConfigFactory.parseString("""
    |  value-1 = v1
    |  value-2 = v2
    |  value-3 = v3
    |  inner {
    |    value-1 = inner_v1
    |    value-2 = inner_v2
    |  }
    |""".stripMargin)

    val empty: TypesafeConfig = ConfigFactory.empty()

    def loadTestFile(path: String): File = {
      val uri = getClass.getResource(path).toURI()
      println(uri)
      Paths.get(uri).toFile()
    }

    def fileConfig = ConfigLoader.toTypesafeConfig(
      configFiles = Seq(config),
      fallback = ConfigFactory.empty(),
    )

    val config = requiredResource(
      "ledger/ledger-runner-common/src/test/resources/testp.conf"
    )

    val testFileConfig = requiredResource(
      "ledger/ledger-runner-common/src/test/resources/test.conf"
    )

    val testFileConfig2 = requiredResource(
      "ledger/ledger-runner-common/src/test/resources/test2.conf"
    )

    def updatedConfig(config: TypesafeConfig, path: String, value: String) =
      config.withValue(path, ConfigValueFactory.fromAnyRef(value))
  }
}

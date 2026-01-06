// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.daml.assistant.config.{PackageConfig, ConfigLoadingError, ConfigParseError}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class JsCodegenRunnerSpec extends AnyFlatSpec with Matchers {
  private val projectRoot = Path.of("/project/root")

  behavior of JsCodegenRunner.getClass.getSimpleName

  private def codegenConf(sdkConfig: String): Either[ConfigLoadingError, JsCodeGenConf] =
    for {
      packageConfig <- PackageConfig.loadFromString(projectRoot, sdkConfig)
      codegenConfig <- JsCodegenRunner.configureFromPackageConfig(packageConfig)
    } yield codegenConfig

  it should "load full js config" in {
    val fullConfig =
      """|
         |name: quickstart
         |version: 1.2.3
         |codegen:
         |  js:
         |    output-directory: path/to/output/js/directory
         |    verbosity: 2
         |    npm-scope: foobar
         |""".stripMargin
    val expected = JsCodeGenConf(
      darFiles = Seq(projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar")),
      outputDirectory = Path.of("path/to/output/js/directory"),
      npmScope = "foobar",
      verbosity = Level.INFO,
    )

    codegenConf(fullConfig) shouldBe Right(expected)
  }

  it should "load required fields only js config" in {
    val requiredFieldsOnlyConfig =
      """|
         |name: quickstart
         |version: 1.2.3
         |codegen:
         |  js:
         |    output-directory: path/to/output/js/directory
         |""".stripMargin
    val expected = JsCodeGenConf(
      darFiles = Seq(projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar")),
      outputDirectory = Path.of("path/to/output/js/directory"),
    )

    codegenConf(requiredFieldsOnlyConfig) shouldBe Right(expected)
  }

  it should "return error if name is missing" in {
    val badConfigStr = """|
       |version: 1.2.3
       |codegen:
       |  js:
       |    output-directory: path/to/output/js/directory""".stripMargin

    codegenConf(badConfigStr) shouldBe Left(ConfigParseError("missing field name"))
  }

  it should "return error if version is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |codegen:
      |  js:
      |    output-directory: path/to/output/js/directory""".stripMargin

    codegenConf(badConfigStr) shouldBe Left(ConfigParseError("missing field version"))
  }

  it should "return error if codegen is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3""".stripMargin

    codegenConf(badConfigStr) shouldBe Left(
      ConfigParseError("Missing required field: DownField(codegen)")
    )
  }

  it should "return error if js is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:""".stripMargin

    codegenConf(badConfigStr) shouldBe Left(
      ConfigParseError(
        "Missing required field: DownField(js),DownField(codegen)"
      )
    )
  }
}

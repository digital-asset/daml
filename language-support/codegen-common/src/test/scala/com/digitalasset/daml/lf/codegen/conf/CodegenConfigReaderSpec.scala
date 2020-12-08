// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.conf

import java.io.File
import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.daml.assistant.config.{ConfigMissing, ConfigParseError, ProjectConfig}
import com.daml.lf.codegen.conf.CodegenConfigReader.{CodegenDest, Java, Result, Scala}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class CodegenConfigReaderSpec extends AnyFlatSpec with Matchers {

  behavior of CodegenConfigReader.getClass.getSimpleName

  private def codegenConf(sdkConfig: String, mode: CodegenDest): Result[Conf] =
    for {
      projectConfig <- ProjectConfig.loadFromString(sdkConfig)
      codegenConfig <- CodegenConfigReader.codegenConf(projectConfig, mode)
    } yield codegenConfig

  private val fullConfig = """|
    |name: quickstart
    |version: 1.2.3
    |codegen:
    |  java:
    |    package-prefix: my.company.java.package
    |    output-directory: path/to/output/java/directory
    |    decoderClass: my.company.java.DecoderClass
    |    verbosity: 1
    |    root:
    |     - java.root1
    |     - java.root2
    |  scala:
    |    package-prefix: my.company.scala.package
    |    output-directory: path/to/output/scala/directory
    |    decoderClass: my.company.scala.DecoderClass
    |    verbosity: 2
    |    root:
    |    - scala,some,string, that can be regex
    |""".stripMargin

  it should "load full java config" in {
    val expected = Conf(
      darFiles = Map(path(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")),
      outputDirectory = path("path/to/output/java/directory"),
      decoderPkgAndClass = Some(("my.company.java", "DecoderClass")),
      verbosity = Level.WARN,
      roots = List("java.root1", "java.root2")
    )

    codegenConf(fullConfig, Java) shouldBe Right(expected)
  }

  it should "load full scala config" in {
    val expected = Conf(
      darFiles = Map(path(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.scala.package")),
      outputDirectory = path("path/to/output/scala/directory"),
      decoderPkgAndClass = Some(("my.company.scala", "DecoderClass")),
      verbosity = Level.INFO,
      roots = List("scala,some,string, that can be regex")
    )

    codegenConf(fullConfig, Scala) shouldBe Right(expected)
  }

  private val requiredFieldsOnlyConfig = """|
    |name: quickstart
    |version: 1.2.3
    |codegen:
    |  java:
    |    package-prefix: my.company.java.package
    |    output-directory: path/to/output/java/directory
    |  scala:
    |    package-prefix: my.company.scala.package
    |    output-directory: path/to/output/scala/directory
    |""".stripMargin

  it should "load required fields only java config" in {
    val expected = Conf(
      darFiles = Map(path(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")),
      outputDirectory = path("path/to/output/java/directory"),
    )

    codegenConf(requiredFieldsOnlyConfig, Java) shouldBe Right(expected)
  }

  it should "load required fields only scala config" in {
    val expected = Conf(
      darFiles = Map(path(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.scala.package")),
      outputDirectory = path("path/to/output/scala/directory"),
    )

    codegenConf(requiredFieldsOnlyConfig, Scala) shouldBe Right(expected)
  }

  it should "return error if name is missing" in {
    val badConfigStr = """|
       |version: 1.2.3
       |codegen:
       |  java:
       |    package-prefix: my.company.java.package
       |    output-directory: path/to/output/java/directory""".stripMargin

    codegenConf(badConfigStr, Java) shouldBe Left(ConfigMissing("name"))
  }

  it should "return error if version is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |codegen:
      |  java:
      |    package-prefix: my.company.java.package
      |    output-directory: path/to/output/java/directory""".stripMargin

    codegenConf(badConfigStr, Java) shouldBe Left(ConfigMissing("version"))
  }

  it should "return error if codegen is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3""".stripMargin

    codegenConf(badConfigStr, Scala) shouldBe Left(
      ConfigParseError("Attempt to decode value on failed cursor: DownField(codegen)"))
  }

  it should "return error if scala is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:
      |  java:
      |    package-prefix: my.company.java.package
      |    output-directory: path/to/output/java/directory""".stripMargin

    codegenConf(badConfigStr, Scala) shouldBe Left(
      ConfigParseError(
        "Attempt to decode value on failed cursor: DownField(scala),DownField(codegen)"))
  }

  private def path(s: String): Path = new File(s).toPath
}

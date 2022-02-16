// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.conf

import java.nio.file.{Path, Paths}

import ch.qos.logback.classic.Level
import com.daml.assistant.config.{ConfigMissing, ConfigParseError, ProjectConfig}
import com.daml.lf.codegen.conf.CodegenConfigReader.{CodegenDest, Java, Result}
import com.daml.lf.data.Ref.{PackageName, PackageVersion}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class CodegenConfigReaderSpec extends AnyFlatSpec with Matchers {

  behavior of CodegenConfigReader.getClass.getSimpleName

  private def codegenConf(sdkConfig: String, mode: CodegenDest): Result[Conf] =
    for {
      projectConfig <- ProjectConfig.loadFromString(projectRoot, sdkConfig)
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
    |""".stripMargin

  it should "load full java config" in {
    val expected = Conf(
      darFiles = Map(
        projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")
      ),
      outputDirectory = path("path/to/output/java/directory"),
      decoderPkgAndClass = Some(("my.company.java", "DecoderClass")),
      verbosity = Level.WARN,
      roots = List("java.root1", "java.root2"),
    )

    codegenConf(fullConfig, Java) shouldBe Right(expected)
  }

  private val requiredFieldsOnlyConfig = """|
    |name: quickstart
    |version: 1.2.3
    |codegen:
    |  java:
    |    package-prefix: my.company.java.package
    |    output-directory: path/to/output/java/directory
    |""".stripMargin

  it should "load required fields only java config" in {
    val expected = Conf(
      darFiles = Map(
        projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")
      ),
      outputDirectory = path("path/to/output/java/directory"),
    )

    codegenConf(requiredFieldsOnlyConfig, Java) shouldBe Right(expected)
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

    codegenConf(badConfigStr, Java) shouldBe Left(
      ConfigParseError("Attempt to decode value on failed cursor: DownField(codegen)")
    )
  }

  it should "return error if java is missing" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:""".stripMargin

    codegenConf(badConfigStr, Java) shouldBe Left(
      ConfigParseError(
        "[A]Option[A]: DownField(java),DownField(codegen)"
      )
    )
  }

  it should "parse package references with >= 1 dash" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:
      |  java:
      |    package-prefix: my.company.java.package
      |    output-directory: path/to/output/java/directory
      |    decoderClass: my.company.java.DecoderClass
      |module-prefixes:
      |  a-0.0.0: a
      |  a-a-0.0.0: a
      |  a-a-a-0.0.0: a
      |  a-a-a-a-0.0.0: a
      |  a-a-a-a-a-0.0.0: a""".stripMargin

    val version = PackageVersion.assertFromString("0.0.0")
    val prefix = "a"

    val expected = Conf(
      darFiles = Map(
        projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")
      ),
      outputDirectory = path("path/to/output/java/directory"),
      decoderPkgAndClass = Some(("my.company.java", "DecoderClass")),
      modulePrefixes = Seq("a", "a-a", "a-a-a", "a-a-a-a", "a-a-a-a-a").view
        .map(x => PackageReference.NameVersion(PackageName.assertFromString(x), version) -> prefix)
        .toMap,
    )

    codegenConf(badConfigStr, Java) shouldBe Right(
      expected
    )
  }

  it should "parse package references with at least one number" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:
      |  java:
      |    package-prefix: my.company.java.package
      |    output-directory: path/to/output/java/directory
      |    decoderClass: my.company.java.DecoderClass
      |module-prefixes:
      |  a-0: a
      |  a-0.0: a
      |  a-0.0.0: a""".stripMargin

    val name = PackageName.assertFromString("a")
    val prefix = "a"

    val expected = Conf(
      darFiles = Map(
        projectRoot.resolve(".daml/dist/quickstart-1.2.3.dar") -> Some("my.company.java.package")
      ),
      outputDirectory = path("path/to/output/java/directory"),
      decoderPkgAndClass = Some(("my.company.java", "DecoderClass")),
      modulePrefixes = Seq("0", "0.0", "0.0.0").view
        .map(x => PackageReference.NameVersion(name, PackageVersion.assertFromString(x)) -> prefix)
        .toMap,
    )

    codegenConf(badConfigStr, Java) shouldBe Right(
      expected
    )
  }

  it should "reject package references with no dash" in {
    val badConfigStr = """|
      |name: quickstart
      |version: 1.2.3
      |codegen:
      |  java:
      |    package-prefix: my.company.java.package
      |    output-directory: path/to/output/java/directory
      |    decoderClass: my.company.java.DecoderClass
      |module-prefixes:
      |  a: a""".stripMargin

    codegenConf(badConfigStr, Java) shouldBe Left(
      ConfigParseError("[K, V]Map[K, V]: DownField(a),DownField(module-prefixes)")
    )
  }

  private def path(s: String): Path = Paths.get(s)

  private val projectRoot = Paths.get("/project/root")
}

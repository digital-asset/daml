// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import com.daml.bazeltools.BazelRunfiles.{requiredResource, rlocation}
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class UpgradesITDev extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  final override protected lazy val devMode = true
  final override protected lazy val enableContractUpgrading = true

  override val majorLanguageVersion: LanguageMajorVersion = LanguageMajorVersion.V2

  // TODO[SW] Migrate existing tests over to inline dars, then remove this
  override protected lazy val darFiles = List(
    rlocation(Paths.get(s"daml-script/test/upgrades-my-templates-v1.dar")),
    rlocation(Paths.get(s"daml-script/test/upgrades-my-templates-v2.dar")),
  )

  lazy val damlScriptDar = requiredResource("daml-script/daml3/daml3-script-2.dev.dar")

  lazy val testFileDir: Path = rlocation(Paths.get("daml-script/test/daml/upgrades/"))

  lazy val testFiles: Seq[Path] = {
    testFileDir.toFile.listFiles(_.getName.endsWith(".daml")).toSeq.map(_.toPath)
  }

  lazy val tempDir: Path = Files.createTempDirectory("upgrades-it-dev")

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    // "run successfully" in {
    testFiles.foreach { testFile =>
      val testName = testFileDir.relativize(testFile).toString.stripSuffix(".daml")
      (s"test file '${testName}'") in {
        for {
          clients <- scriptClients(provideAdminPorts = true)
          testDarPath = buildTestingDar(testFileDir, testName, testFile)
          //   // TODO[SW] Upload `dars` to the participant, using CantonFixtures defaultLedgerClient
          testDar = CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V2))
          _ <- run(
            clients,
            QualifiedName.assertFromString(s"${testName}:main"),
            dar = testDar,
            enableContractUpgrading = true,
          )
        } yield succeed
      }
    }
  }

  import io.circe._
  import io.circe.yaml
  import scala.util.matching.Regex
  import scala.sys.process._

  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  private val damlc = requiredResource(s"compiler/damlc/damlc$exe")

  case class PackageDefinition(
      name: String,
      versions: Int,
      modules: Map[String, String],
  )

  // TODO[SW] Consider another attempt at using io.circe.generic.auto._
  implicit val decodePackageDefinition: Decoder[PackageDefinition] =
    new Decoder[PackageDefinition] {
      final def apply(c: HCursor): Decoder.Result[PackageDefinition] =
        for {
          name <- c.downField("name").as[String]
          versions <- c.downField("versions").as[Int]
          modules <- c
            .downField("modules")
            .as(
              Decoder
                .decodeList(Decoder.forProduct2("name", "contents") { (x: String, y: String) =>
                  (x, y)
                })
                .map(Map.from _)
            )
        } yield {
          new PackageDefinition(name, versions, modules)
        }
    }

  val packagePattern: Regex = "\\{- PACKAGE *\n((?:.|[\r\n])+?)-\\}".r

  def readPackageDefinitions(fileContent: String): Seq[PackageDefinition] = {
    packagePattern.findAllMatchIn(fileContent).toSeq.map { m =>
      yaml.parser
        .parse(m.group(1))
        .left
        .map(err => err: Error)
        .flatMap(_.as[PackageDefinition])
        .fold(throw _, identity)
    }
  }

  def macroDef(n: Int) =
    s"""#define V$n(code)
       |#if DU_VERSION == $n
       |  #define V$n(code) code
       |#endif
       |""".stripMargin

  def buildModules(p: PackageDefinition, dir: Path) =
    p.modules.foreach { case (moduleFullName, content) =>
      val path = dir.resolve(Paths.get(moduleFullName.replace(".", "/") + ".daml"))
      val moduleName = moduleFullName.substring(moduleFullName.lastIndexOf(".") + 1)
      val fileContent = "{-# LANGUAGE CPP #-}\n" + (1 to p.versions)
        .map(macroDef _)
        .mkString + "\nmodule " + moduleName + " where\n\n" + content
      Files.createDirectories(path.getParent());
      Files.write(path, fileContent.getBytes(StandardCharsets.UTF_8))
    }

  def buildDar(
      dir: Path,
      name: String,
      version: Int,
      dataDeps: Seq[DataDep] = Seq.empty,
      opts: Seq[String] = Seq.empty,
  ): Path = {
    writeDamlYaml(dir, name, version, dataDeps)
    (Seq(
      damlc.toString,
      "build",
      "--project-root",
      dir.toString,
    ) ++ opts).! shouldBe 0
    dir.resolve(s".daml/dist/${name}-$version.0.0.dar")
  }

  def buildPackages(p: PackageDefinition, tmpDir: Path): Seq[DataDep] = {
    val dir = tmpDir.resolve("daml-package-" + p.name)
    buildModules(p, dir)
    (1 to p.versions).toSeq.map { version =>
      DataDep(
        versionedName = s"${p.name}-${version}.0.0",
        path =
          buildDar(dir, p.name, version, opts = Seq("--ghc-option", s"-DDU_VERSION=${version}")),
        prefix = s"V${version}",
      )
    }
  }

  case class DataDep(
      versionedName: String,
      path: Path,
      prefix: String,
  )

  def writeDamlYaml(
      dir: Path,
      name: String,
      version: Int,
      dataDeps: Seq[DataDep] = Seq.empty,
  ) = {
    val fileContent =
      s"""sdk-version: 0.0.0
         |build-options: [--target=2.dev]
         |name: $name
         |source: .
         |version: $version.0.0
         |dependencies:
         |  - daml-prim
         |  - daml-stdlib
         |  - ${damlScriptDar.toString}
         |data-dependencies:
         |${darFiles.map("  - " + _).mkString("\n")}
         |${dataDeps.map(d => "  - " + d.path.toString).mkString("\n")}
         |module-prefixes:
         |  upgrades-my-templates-1.0.0: V1
         |  upgrades-my-templates-2.0.0: V2
         |${dataDeps.map(d => s"  ${d.versionedName}: ${d.prefix}").mkString("\n")}
         |""".stripMargin
    Files.write(dir.resolve("daml.yaml"), fileContent.getBytes(StandardCharsets.UTF_8))
  }

  // For a given test file, builds all upgrade data packages, then builds test dar depending on those.
  def buildTestingDar(testFileDir: Path, testName: String, testFile: Path): Path = {
    val projName = s"daml-upgrades-test-$testName"
    val testRoot = Files.createDirectory(tempDir.resolve(projName))
    val testProj = Files.createDirectory(testRoot.resolve("proj"))
    val dars =
      for {
        pkgDef <- readPackageDefinitions(Files.readString(testFile))
        dar <- buildPackages(pkgDef, testRoot)
      } yield dar
    Files.copy(testFile, testProj.resolve(testFileDir.relativize(testFile)))
    buildDar(testProj, projName, 1, dars)
  }
}

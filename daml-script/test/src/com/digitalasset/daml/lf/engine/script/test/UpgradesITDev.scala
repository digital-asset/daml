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

  lazy val testFiles: Path = rlocation(Paths.get("daml-script/test/daml/upgrades/"))

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    "run successfully" in {
      for {
        clients <- scriptClients(provideAdminPorts = true)
        (testDarPath, dars, tmpDir) = buildTestingDar(testFiles)
        // TODO[SW] Upload `dars` to the participant, using CantonFixtures defaultLedgerClient
        testDar = CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V2))
        _ <- run(
          clients,
          QualifiedName.assertFromString("UpgradesTest:main"),
          dar = testDar,
          enableContractUpgrading = true,
        )
        // _ = Files.delete(tmpDir)
      } yield succeed
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
      deps: Seq[String] = Seq.empty,
      opts: Seq[String] = Seq.empty,
  ): Path = {
    writeDamlYaml(dir, name, version, deps)
    (Seq(
      damlc.toString,
      "build",
      "--project-root",
      dir.toString,
    ) ++ opts).! shouldBe 0
    dir.resolve(s".daml/dist/${name}-$version.0.0.dar")
  }

  def buildPackages(p: PackageDefinition, tmpDir: Path): Seq[Path] = {
    val dir = tmpDir.resolve("daml-package-" + p.name)
    buildModules(p, dir)
    (1 to p.versions).toSeq.map(version =>
      buildDar(dir, p.name, version, opts = Seq("--ghc-option", s"-DDU_VERSION=$version"))
    )
  }

  def writeDamlYaml(dir: Path, name: String, version: Int, deps: Seq[String] = Seq.empty) = {
    val fileContent =
      s"""sdk-version: 0.0.0
         |build-options: [--target=2.dev]
         |name: $name
         |source: .
         |version: $version.0.0
         |dependencies:
         |  - daml-prim
         |  - daml-stdlib
         |${deps.map("  - " + _).mkString("\n")}
         |data-dependencies:
         |${darFiles.map("  - " + _).mkString("\n")}
         |module-prefixes:
         |  upgrades-my-templates-1.0.0: V1
         |  upgrades-my-templates-2.0.0: V2
         |""".stripMargin
    Files.write(dir.resolve("daml.yaml"), fileContent.getBytes(StandardCharsets.UTF_8))
  }

  // Iterates all test files, builds all upgrade data packages, then builds test dar depending on those.
  def buildTestingDar(testFiles: Path): (Path, Seq[Path], Path) = {
    val damlFiles = testFiles.toFile.listFiles(_.getName.endsWith(".daml")).toSeq.map(_.toPath)
    val dir = Files.createTempDirectory("daml-upgrades-test")
    val dars = damlFiles
      .map(file =>
        readPackageDefinitions(Files.readString(file)).map(buildPackages(_, dir)).flatten
      )
      .flatten
    damlFiles.foreach { file => Files.copy(file, dir.resolve(testFiles.relativize(file))) }
    (buildDar(dir, "daml-upgrades-test", 1, dars.map(_.toString) :+ damlScriptDar.toString), dars, dir)
  }
}

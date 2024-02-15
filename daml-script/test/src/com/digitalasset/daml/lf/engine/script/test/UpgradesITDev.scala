// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import io.circe._
import io.circe.yaml
import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import com.daml.bazeltools.BazelRunfiles.{requiredResource, rlocation}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.AdminLedgerClient
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scala.concurrent.Future
import scala.sys.process._
import scala.util.matching.Regex
import scala.collection.mutable.Map
import scala.concurrent.duration.DurationInt

class UpgradesITDev extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  final override protected lazy val devMode = true
  final override protected lazy val enableContractUpgrading = true

  override val majorLanguageVersion: LanguageMajorVersion = LanguageMajorVersion.V2

  override protected lazy val darFiles = List()

  lazy val damlScriptDar = requiredResource("daml-script/daml3/daml3-script-2.dev.dar")
  lazy val upgradeTestLibDar: Path = rlocation(Paths.get("daml-script/test/upgrade-test-lib.dar"))

  lazy val tempDir: Path = Files.createTempDirectory("upgrades-it-dev")

  val testFileDir: Path = rlocation(Paths.get("daml-script/test/daml/upgrades/"))
  val testCases: Seq[TestCase] = getTestCases(testFileDir)

  private def traverseSequential[A, B](elems: Seq[A])(f: A => Future[B]): Future[Seq[B]] =
    elems.foldLeft(Future.successful(Seq.empty[B])) { case (comp, elem) =>
      comp.flatMap { elems => f(elem).map(elems :+ _) }
    }

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    testCases.foreach { testCase =>
      testCase.name in {
        for {
          // Build dars
          (testDarPath, deps) <- Future { buildTestCaseDar(testCase) }

          // Connection
          clients <- scriptClients(provideAdminPorts = true)
          adminClients = ledgerPorts.map { portInfo =>
            (
              portInfo.ledgerPort.value,
              AdminLedgerClient.singleHost(
                "localhost",
                portInfo.adminPort.value,
                None,
                LedgerClientChannelConfiguration.InsecureDefaults,
              ),
            )
          }

          _ <- traverseSequential(adminClients) { case (ledgerPort, adminClient) =>
            Future.traverse(deps) { dep =>
              Thread.sleep(500)
              println(
                s"Uploading ${dep.versionedName} to participant on port ${ledgerPort}"
              )
              adminClient
                .uploadDar(dep.path, dep.versionedName)
                .map(_.left.map(msg => throw new Exception(msg)))
            }
          }

          // Wait for upload
          _ <- RetryStrategy.constant(attempts = 20, waitTime = 1.seconds) { (_, _) =>
            assertDepsVetted(adminClients.head._2, deps)
          }
          _ = println("All packages vetted on all participants")

          // Run tests
          testDar = CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V2))
          _ <- run(
            clients,
            QualifiedName.assertFromString(s"${testCase.name}:main"),
            dar = testDar,
            enableContractUpgrading = true,
          )
        } yield succeed
      }
    }
  }

  private def assertDepsVetted(client: AdminLedgerClient, deps: Seq[DataDep]): Future[Unit] = {
    client
      .listVettedPackages()
      .map(_.foreach { case (participantId, packageIds) =>
        deps.foreach { dep =>
          if (!packageIds.contains(dep.packageId))
            throw new Exception(
              s"Couldn't find package ${dep.versionedName} on participant $participantId"
            )
        }
      })
  }

  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  private val damlc = requiredResource(s"compiler/damlc/damlc$exe")

  case class PackageDefinition(
      name: String,
      versions: Int,
      modules: Map[String, String],
  )

  // TODO[SW] Consider another attempt at using io.circe.generic.auto._
  // [MA] we make this lazy because we're calling it from the top level before
  // the entire class has finished loading
  implicit lazy val decodePackageDefinition: Decoder[PackageDefinition] =
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

  lazy val packagePattern: Regex = "\\{- PACKAGE *\n((?:.|[\r\n])+?)-\\}".r

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
      val path =
        buildDar(dir, p.name, version, opts = Seq("--ghc-option", s"-DDU_VERSION=${version}"))
      DataDep(
        versionedName = s"${p.name}-${version}.0.0",
        path,
        prefix = s"V${version}",
        packageId =
          PackageId.assertFromString(DarParser.assertReadArchiveFromFile(path.toFile).main.getHash),
      )
    }
  }

  case class DataDep(
      versionedName: String,
      path: Path,
      prefix: String,
      packageId: PackageId,
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
         |  - ${upgradeTestLibDar.toString}
         |${dataDeps.map(d => "  - " + d.path.toString).mkString("\n")}
         |module-prefixes:
         |${dataDeps.map(d => s"  ${d.versionedName}: ${d.prefix}").mkString("\n")}
         |""".stripMargin
    Files.write(dir.resolve("daml.yaml"), fileContent.getBytes(StandardCharsets.UTF_8))
  }

  def buildTestCaseDar(testCase: TestCase): (Path, Seq[DataDep]) = {
    val testCaseRoot = Files.createDirectory(tempDir.resolve(testCase.name))
    val testCasePkg = Files.createDirectory(testCaseRoot.resolve("test-case"))
    val dars = testCase.pkgDefs.flatMap(buildPackages(_, testCaseRoot))
    Files.copy(testCase.damlPath, testCasePkg.resolve(testCase.damlRelPath))
    val testDar = buildDar(testCasePkg, testCase.name, 1, dars)
    (testDar, dars)
  }

  case class TestCase(
      name: String,
      damlPath: Path,
      damlRelPath: Path,
      pkgDefs: Seq[PackageDefinition],
  )

  // Ensures no package name is defined twice across all test files
  def getTestCases(testFileDir: Path): Seq[TestCase] = {
    val cases = getTestCasesUnsafe(testFileDir)
    val packageNameDefiners = Map[String, Seq[String]]()
    for {
      c <- cases
      pkg <- c.pkgDefs
    } packageNameDefiners.updateWith(pkg.name) {
      case None => Some(Seq(c.name))
      case Some(names) => Some(names :+ c.name)
    }
    packageNameDefiners.foreach {
      case (packageName, caseNames) if (caseNames.length > 1) =>
        throw new IllegalArgumentException(
          s"Package with name $packageName is defined multiple times within the following case(s): ${caseNames.distinct
              .mkString(",")}"
        )
      case _ =>
    }
    cases
  }

  def isTest(file: File): Boolean =
    file.getName.endsWith(".daml")

  def getTestCasesUnsafe(testFileDir: Path): Seq[TestCase] =
    testFileDir.toFile.listFiles(isTest _).toSeq.map { testFile =>
      val damlPath = testFile.toPath
      val damlRelPath = testFileDir.relativize(damlPath)
      TestCase(
        name = damlRelPath.toString.stripSuffix(".daml"),
        damlPath,
        damlRelPath,
        pkgDefs = readPackageDefinitions(Files.readString(damlPath)),
      )
    }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import io.circe._
import io.circe.yaml
import java.io.File
import java.nio.file.{Files, Path, Paths}
import com.daml.bazeltools.BazelRunfiles.{requiredResource, rlocation}
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.engine.script.test.DarUtil.{buildDar, Dar, DataDep}
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.test.TestingAdminLedgerClient
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scala.concurrent.Future
import scala.sys.process._
import scala.util.matching.Regex
import scala.collection.mutable
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
              TestingAdminLedgerClient.singleHost(
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
                .uploadDar(dep.path.toFile)
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

  private def assertDepsVetted(
      client: TestingAdminLedgerClient,
      deps: Seq[Dar],
  ): Future[Unit] = {
    client
      .listVettedPackages()
      .map(_.foreach { case (participantId, packageIds) =>
        deps.foreach { dep =>
          if (!packageIds.contains(dep.mainPackageId))
            throw new Exception(
              s"Couldn't find package ${dep.versionedName} on participant $participantId"
            )
        }
      })
  }

  def macroDef(n: Int) =
    s"""#define V$n(code)
       |#if DU_VERSION == $n
       |  #define V$n(code) code
       |#endif
       |""".stripMargin

  def buildPackages(p: PackageDefinition, tmpDir: Path): Seq[Dar] = {
    val dir = Files.createDirectory(tmpDir.resolve("daml-package-" + p.name))
    (1 to p.versions).toSeq.map { version =>
      assertBuildDar(
        name = p.name,
        version,
        modules = p.modules.map { case (moduleName, content) =>
          val fileContent = "{-# LANGUAGE CPP #-}\n" + (1 to p.versions)
            .map(macroDef _)
            .mkString + "\nmodule " + moduleName + " where\n\n" + content
          (moduleName, fileContent)
        },
        deps = Seq(damlScriptDar.toPath),
        opts = Seq("--ghc-option", s"-DDU_VERSION=${version}"),
        tmpDir = Some(dir),
      )
    }
  }

  def buildTestCaseDar(testCase: TestCase): (Path, Seq[Dar]) = {
    val testCaseRoot = Files.createDirectory(tempDir.resolve(testCase.name))
    val testCasePkg = Files.createDirectory(testCaseRoot.resolve("test-case"))
    val dars: Seq[Dar] = testCase.pkgDefs.flatMap(buildPackages(_, testCaseRoot))

    val darPath = assertBuildDar(
      name = testCase.name,
      modules = Map((testCase.name, Files.readString(testCase.damlPath))),
      dataDeps = Seq(DataDep(upgradeTestLibDar)) :++ dars.map { dar =>
        DataDep(
          path = dar.path,
          prefix = Some((dar.versionedName, s"V${dar.version}")),
        )
      },
      tmpDir = Some(testCasePkg),
    ).path
    (darPath, dars)
  }

  def assertBuildDar(
      name: String,
      version: Int = 1,
      modules: Map[String, String],
      deps: Seq[Path] = Seq.empty,
      dataDeps: Seq[DataDep] = Seq.empty,
      opts: Seq[String] = Seq(),
      tmpDir: Option[Path] = None,
  ): Dar = {
    val builder = new StringBuilder
    def log(t: String)(s: String) = discard(builder.append(s"${t}: ${s}\n"))
    buildDar(
      name = name,
      version = version,
      modules = modules,
      deps = deps,
      dataDeps = dataDeps,
      opts = opts,
      tmpDir = tmpDir,
      logger = ProcessLogger(log("stdout"), log("stderr")),
    ) match {
      case Right(dar) => dar
      case Left(exitCode) =>
        fail(
          s"While building ${name}-${version}.0.0: 'daml build' exited with ${exitCode}\n${builder.toString}"
        )
    }
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
    val packageNameDefiners = mutable.Map[String, Seq[String]]()
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
        pkgDefs = PackageDefinition.readFromFile(damlPath),
      )
    }

  case class PackageDefinition(
      name: String,
      versions: Int,
      modules: Map[String, String],
  )

  object PackageDefinition {

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

    def readFromFile(path: Path): Seq[PackageDefinition] = {
      packagePattern.findAllMatchIn(Files.readString(path)).toSeq.map { m =>
        yaml.parser
          .parse(m.group(1))
          .left
          .map(err => err: Error)
          .flatMap(_.as[PackageDefinition])
          .fold(throw _, identity)
      }
    }
  }
}

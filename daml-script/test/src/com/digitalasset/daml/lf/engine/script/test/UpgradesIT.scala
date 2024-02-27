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
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
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

class UpgradesIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  final override protected lazy val devMode = true
  final override protected lazy val enableContractUpgrading = true

  val languageVersion: LanguageVersion = LanguageVersion.v2_1
  override val majorLanguageVersion: LanguageMajorVersion = languageVersion.major

  override protected lazy val darFiles = List()

  lazy val damlScriptDar = requiredResource("daml-script/daml3/daml3-script-2.1.dar")
  lazy val upgradeTestLibDar: Path = rlocation(Paths.get("daml-script/test/upgrade-test-lib.dar"))

  lazy val tempDir: Path = Files.createTempDirectory("upgrades-it")

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
          (testDarPath, deps) <- buildTestCaseDar(testCase)

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

  def buildTestCaseDar(testCase: TestCase): Future[(Path, Seq[Dar])] = Future {
    val testCaseRoot = Files.createDirectory(tempDir.resolve(testCase.name))
    val testCasePkg = Files.createDirectory(testCaseRoot.resolve("test-case"))
    val dars: Seq[Dar] = testCase.pkgDefs.map(_.build(testCaseRoot))

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
      lfVersion = languageVersion,
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
      case (packageName, caseNames) if (caseNames.distinct.length > 1) =>
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
      version: Int,
      modules: Map[String, String],
  ) {
    def build(tmpDir: Path): Dar = {
      assertBuildDar(
        name = this.name,
        version = this.version,
        modules = this.modules,
        deps = Seq(damlScriptDar.toPath),
        tmpDir = Some(tmpDir),
      )
    }
  }

  object PackageDefinition {

    case class PackageComment(
        name: String,
        versions: Int,
    )

    // TODO[SW] Consider another attempt at using io.circe.generic.auto._
    // [MA] we make this lazy because we're calling it from the top level before
    // the entire class has finished loading
    implicit lazy val decodePackageComment: Decoder[PackageComment] =
      new Decoder[PackageComment] {
        final def apply(c: HCursor): Decoder.Result[PackageComment] =
          for {
            name <- c.downField("name").as[String]
            versions <- c.downField("versions").as[Int]
          } yield {
            new PackageComment(name, versions)
          }
      }

    case class ModuleComment(
        packageName: String,
        contents: String,
    )

    implicit lazy val decodeModuleComment: Decoder[ModuleComment] =
      new Decoder[ModuleComment] {
        final def apply(c: HCursor): Decoder.Result[ModuleComment] =
          for {
            packageName <- c.downField("package").as[String]
            contents <- c.downField("contents").as[String]
          } yield {
            new ModuleComment(packageName, contents)
          }
      }

    lazy val packagePattern: Regex = "(?:^|\n)\\{- PACKAGE *\n((?:.|[\r\n])+?)\n-\\}".r
    lazy val modulePattern: Regex = "(?:^|\n)\\{- MODULE *\n((?:.|[\r\n])+?)\n-\\}".r

    def readFromFile(path: Path): Seq[PackageDefinition] = {
      val fileContents = Files.readString(path)
      val packageComments =
        packagePattern.findAllMatchIn(fileContents).toSeq.map { m =>
          yaml.parser
            .parse(m.group(1))
            .left
            .map(err => err: Error)
            .flatMap(_.as[PackageComment])
            .fold(throw _, identity)
        }
      val moduleMap: Map[String, Seq[Seq[VersionedLine]]] =
        modulePattern
          .findAllMatchIn(fileContents)
          .toSeq
          .map { m =>
            yaml.parser
              .parse(m.group(1))
              .left
              .map(err => err: Error)
              .flatMap(_.as[ModuleComment])
              .fold(throw _, identity)
          }
          .groupMap(_.packageName)(c => readVersionedLines(c.contents))

      packageComments.flatMap { c =>
        (1 to c.versions).toSeq.map { version =>
          PackageDefinition(
            name = c.name,
            version = version,
            modules = moduleMap
              .getOrElse(c.name, Seq.empty)
              .map(getVersionedModule(c.name, _, version))
              .groupMap(_._1)(_._2)
              .map { case (modName, modDefs) =>
                assertUnique(c.name, modName, modDefs)
              },
          )
        }
      }
    }

    case class VersionedLine(
        line: String,
        versions: Option[Seq[Int]],
    )

    def readVersionedLines(contents: String): Seq[VersionedLine] = {
      val versionedLinePat: Regex = "-- @V(.*)$".r
      val intPat: Regex = "\\d+".r
      contents.split('\n').toSeq.map { line =>
        VersionedLine(
          line = line,
          versions = versionedLinePat.findFirstMatchIn(line).map { m =>
            intPat.findAllMatchIn(m.group(1)).toSeq.map(_.group(0).toInt)
          },
        )
      }
    }

    def getVersionedModule(
        packageName: String,
        lines: Seq[VersionedLine],
        version: Int,
    ): (String, String) = {
      val modNamePat: Regex = "module +([^ ]+) +where".r
      val contents = lines
        .collect { case vl if vl.versions.fold(true)(_.contains(version)) => vl.line }
        .mkString("\n")
      modNamePat.findFirstMatchIn(contents) match {
        case Some(m) => (m.group(1), contents)
        case None =>
          fail(
            s"Failed to extract module name for a MODULE with package = ${packageName}"
          )
      }
    }

    def assertUnique(
        packageName: String,
        modName: String,
        modDefs: Seq[String],
    ): (String, String) = {
      modDefs match {
        case Seq(modDef) => (modName, modDef)
        case _ =>
          fail(
            s"Multiple conflicting definitions of module ${modName} in package ${packageName}"
          )
      }
    }
  }
}

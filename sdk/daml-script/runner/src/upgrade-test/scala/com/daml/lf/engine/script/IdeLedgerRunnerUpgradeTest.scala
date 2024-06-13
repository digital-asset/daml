// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles.{requiredResource, rlocation}
import com.daml.lf.engine.script.test.DarUtil.{buildDar, Dar, DataDep}
import com.daml.lf.language.LanguageVersion
import com.daml.scalautil.Statement.discard
import io.circe._
import io.circe.yaml
import java.io.File
import java.nio.file.{Files, Path, Paths}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.Suite
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.sys.process.ProcessLogger
import scala.util.matching.Regex

final class IdeLedgerRunnerUpgradeTest extends AsyncWordSpec with RunnerTestBase {
  self: Suite =>

  val languageVersion: LanguageVersion = LanguageVersion.v1_dev

  lazy val damlScriptDar = requiredResource("daml-script/daml3/daml3-script-1.dev.dar")
  lazy val upgradeTestLibDar: Path = rlocation(Paths.get("daml-script/runner/upgrade-test-lib.dar"))

  lazy val tempDir: Path = Files.createTempDirectory("ide-upgrade-test")

  val testFileDir: Path = rlocation(Paths.get("daml-script/runner/src/upgrade-test/daml/test/"))
  val testCases: Seq[TestCase] = getTestCases(testFileDir)

  "daml-script upgrades on IDE Ledger" should {
    testCases.foreach { testCase =>
      testCase.name in {
        for {
          testDarPath <- buildTestCaseDar(testCase)
          r <- testDamlScript(
            testDarPath,
            Seq(
              "--ide-ledger",
              s"--script-name=${testCase.name}:main",
              "--enable-contract-upgrading",
            ),
            Right(Seq()),
          )
        } yield r
      }
    }
  }

  def buildTestCaseDar(testCase: TestCase): Future[Path] = Future {
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
    darPath
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
    import java.lang.management.ManagementFactory
    println(ManagementFactory.getRuntimeMXBean().getInputArguments())
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

    private def findComments(commentTitle: String, lines: Seq[String]): Seq[String] =
      lines.foldLeft((None: Option[String], Seq[String]())) {
        case ((None, cs), line) if line.startsWith(s"{- $commentTitle") =>
          (Some(""), cs)
        case ((None, cs), _) =>
          (None, cs)
        case ((Some(str), cs), line) if line.startsWith("-}") =>
          (None, cs :+ str)
        case ((Some(str), cs), line) =>
          (Some(str + "\n" + line), cs)
      } match {
        case (None, cs) => cs
        case (Some(str), _) =>
          throw new IllegalArgumentException(
            s"Missing \"-}\" to close $commentTitle containing\n$str"
          )
      }

    def readFromFile(path: Path): Seq[PackageDefinition] = {
      val fileLines = Files.readAllLines(path).asScala.toSeq
      val packageComments =
        findComments("PACKAGE", fileLines).map { comment =>
          yaml.parser
            .parse(comment)
            .left
            .map(err => err: Error)
            .flatMap(_.as[PackageComment])
            .fold(throw _, identity)
        }
      val moduleMap: Map[String, Seq[Seq[VersionedLine]]] =
        findComments("MODULE", fileLines)
          .map { comment =>
            yaml.parser
              .parse(comment)
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

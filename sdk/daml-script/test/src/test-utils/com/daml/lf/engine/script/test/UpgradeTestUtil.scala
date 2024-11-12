// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.engine.script.test.DarUtil.{Dar, DataDep, buildDar}
import com.daml.lf.language.LanguageVersion
import com.daml.scalautil.Statement.discard
import io.circe.{yaml, _}

import java.io.File
import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.matching.Regex

class UpgradeTestUtil(upgradeTestLibDar: Path)(implicit executor: ExecutionContext) {
  import UpgradeTestUtil._

  private val tempDir: Path = Files.createTempDirectory("upgrades-it")

  private val builtTestCaseDars: mutable.HashMap[TestCase, (Path, Seq[Dar])] =
    new mutable.HashMap[TestCase, (Path, Seq[Dar])]

  def buildTestCaseDarMemoized(testCase: TestCase): Future[(Path, Seq[Dar])] =
    builtTestCaseDars
      .get(testCase)
      .fold(buildTestCaseDar(testCase).map { res =>
        builtTestCaseDars.update(testCase, res)
        res
      })(Future.successful(_))

  private def buildTestCaseDar(testCase: TestCase): Future[(Path, Seq[Dar])] = Future {
    val testCaseRoot = Files.createDirectory(tempDir.resolve(testCase.name))
    val testCasePkg = Files.createDirectory(testCaseRoot.resolve("test-case"))
    // Each dar is tagged with whether it needs to be uploaded, to save time on duplicate uploads
    val dars: Seq[(Dar, Boolean)] = testCase.buildPkgDefs(testCaseRoot)
    // For package preference tests, we need to know package ids
    // These cannot be hard coded due to sdk version changes
    val packageIdsModuleSource =
      s"""module PackageIds where
         |import qualified DA.Map as Map
         |import DA.Optional (fromSomeNote)
         |import UpgradeTestLib (PackageId (..))
         |packageIds : Map.Map Text PackageId
         |packageIds = Map.fromList [${dars
          .map { case (dar, _) => s"(\"${dar.versionedName}\",PackageId \"${dar.mainPackageId}\")" }
          .mkString(",")}]
         |getPackageId : Text -> PackageId
         |getPackageId name = fromSomeNote ("Couldn't find package id of " <> name) $$ Map.lookup name packageIds
      """.stripMargin

    val darPath = assertBuildDar(
      name = testCase.name,
      version = 1,
      lfVersion = LanguageVersion.v1_dev,
      modules = Map(
        (testCase.name, Files.readString(testCase.damlPath)),
        ("PackageIds", packageIdsModuleSource),
      ),
      deps = Seq.empty,
      dataDeps = Seq(DataDep(upgradeTestLibDar)) :++ dars.map { case (dar, _) =>
        DataDep(
          path = dar.path,
          prefix = Some((dar.versionedName, s"V${dar.version}")),
        )
      },
      opts = Seq.empty,
      tmpDir = Some(testCasePkg),
    ).path
    (darPath, dars.filter(_._2).map(_._1))
  }
}

object UpgradeTestUtil {

  case class BuildError(reason: String) extends RuntimeException(reason)

  case class TestCase(
      name: String,
      damlPath: Path,
      damlRelPath: Path,
      pkgDefs: Seq[PackageDefinition],
  ) {
    private val unitIdMap = pkgDefs.map(pd => (s"${pd.name}-${pd.version}.0.0", pd)).toMap

    private val sortedPkgDefs = {
      val res = mutable.ListBuffer[(PackageDefinition, String)]()
      def sortPkgDef(pkgName: String, seen: Set[String] = Set[String]()): Unit = {
        if (seen(pkgName)) throw BuildError(s"Cycle detected: ${seen + pkgName}")
        if (!res.exists { case (_, name) => name == pkgName }) {
          val pkgDef = unitIdMap.getOrElse(
            pkgName,
            throw BuildError(s"Package $pkgName is not defined in this file"),
          )
          pkgDef.depends.foreach(sortPkgDef(_, seen + pkgName))
          discard(res += ((pkgDef, pkgName)))
        }
      }
      unitIdMap.keys.foreach(sortPkgDef(_))
      res.map(_._1).toSeq
    }

    private[UpgradeTestUtil] def buildPkgDefs(tmpDir: Path): Seq[(Dar, Boolean)] = {
      val builtDars: mutable.Map[String, Dar] = mutable.Map[String, Dar]()
      val topLevelDars: mutable.Set[Dar] = mutable.Set[Dar]()
      sortedPkgDefs.foreach { pd =>
        val deps = pd.depends.map(
          builtDars.getOrElse(
            _,
            throw BuildError("Impossible missing dependency while building"),
          )
        )
        val builtDar = pd.build(tmpDir, deps)
        discard(builtDars += ((builtDar.versionedName, builtDar)))
        discard(topLevelDars subtractAll deps)
        topLevelDars += builtDar
      }
      builtDars.values
        .map(dar => (dar, topLevelDars(dar)))
        .toSeq
        .sortBy(_._1.versionedName)(Ordering[String].reverse)
    }
  }

  private def assertBuildDar(
      name: String,
      version: Int,
      lfVersion: LanguageVersion,
      modules: Map[String, String],
      deps: Seq[Path],
      dataDeps: Seq[DataDep],
      opts: Seq[String],
      tmpDir: Option[Path],
  ): Dar = {
    val builder = new StringBuilder
    def log(t: String)(s: String) = discard(builder.append(s"${t}: ${s}\n"))
    buildDar(
      name = name,
      version = version,
      lfVersion = lfVersion,
      modules = modules,
      deps = deps,
      dataDeps = dataDeps,
      opts = opts,
      tmpDir = tmpDir,
      logger = ProcessLogger(log("stdout"), log("stderr")),
    ) match {
      case Right(dar) => dar
      case Left(exitCode) =>
        throw BuildError(
          s"While building ${name}-${version}.0.0: 'daml build' exited with ${exitCode}\n${builder.toString}"
        )
    }
  }

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
        throw BuildError(
          s"Package with name $packageName is defined multiple times within the following case(s): ${caseNames.distinct
              .mkString(",")}"
        )
      case _ =>
    }
    cases
  }

  private def isTest(file: File): Boolean =
    file.getName.endsWith(".daml")

  private def getTestCasesUnsafe(testFileDir: Path): Seq[TestCase] =
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
      lfVersion: LanguageVersion,
      depends: Seq[String], // List of unit ids of dependencies
      modules: Map[String, String],
  ) {
    private[UpgradeTestUtil] def build(tmpDir: Path, dataDeps: Seq[Dar] = Seq[Dar]()): Dar = {
      assertBuildDar(
        name = this.name,
        version = this.version,
        lfVersion = this.lfVersion,
        modules = this.modules,
        deps = Seq.empty,
        dataDeps =
          dataDeps.map(dar => DataDep(dar.path, Some((dar.versionedName, s"V${dar.version}")))),
        tmpDir = Some(tmpDir),
        opts = Seq.empty,
      )
    }
  }

  object PackageDefinition {

    private case class PackageComment(
        name: String,
        versions: Int,
        lfVersion: Option[String],
        depends: String,
    )

    // TODO[SW] Consider another attempt at using io.circe.generic.auto._
    // [MA] we make this lazy because we're calling it from the top level before
    // the entire class has finished loading
    private implicit lazy val decodePackageComment: Decoder[PackageComment] =
      new Decoder[PackageComment] {
        final def apply(c: HCursor): Decoder.Result[PackageComment] =
          for {
            name <- c.downField("name").as[String]
            versions <- c.downField("versions").as[Int]
            lfVersion <- c.downField("lf-version").as[Option[String]]
            depends <- c.downField("depends").as[Option[String]].map(_.getOrElse(""))
          } yield {
            PackageComment(name, versions, lfVersion, depends)
          }
      }

    private case class ModuleComment(
        packageName: String,
        contents: String,
    )

    private implicit lazy val decodeModuleComment: Decoder[ModuleComment] =
      new Decoder[ModuleComment] {
        final def apply(c: HCursor): Decoder.Result[ModuleComment] =
          for {
            packageName <- c.downField("package").as[String]
            contents <- c.downField("contents").as[String]
          } yield {
            ModuleComment(packageName, contents)
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
          throw BuildError(
            s"Missing \"-}\" to close $commentTitle containing\n$str"
          )
      }

    private[UpgradeTestUtil] def readFromFile(path: Path): Seq[PackageDefinition] = {
      val fileLines = Files.readAllLines(path).asScala.toSeq
      val packageComments =
        findComments("PACKAGE", fileLines).map { comment =>
          yaml.parser
            .parse(comment)
            .left
            .map(err => err: Error)
            .flatMap(_.as[PackageComment])
            .fold(error => throw BuildError(error.getMessage), identity)
        }
      val moduleMap: Map[String, Seq[Seq[VersionedLine]]] =
        findComments("MODULE", fileLines)
          .map { comment =>
            yaml.parser
              .parse(comment)
              .left
              .map(err => err: Error)
              .flatMap(_.as[ModuleComment])
              .fold(error => throw BuildError(error.getMessage), identity)
          }
          .groupMap(_.packageName)(c => readVersionedLines(c.contents))

      packageComments.flatMap { c =>
        val versionedLfVersion = c.lfVersion.map(readVersionedLines(_))
        val versionedDepends = readVersionedLines(c.depends)
        (1 to c.versions).map { version =>
          PackageDefinition(
            name = c.name,
            version = version,
            lfVersion = versionedLfVersion.fold(LanguageVersion.v1_dev)(vLf =>
              LanguageVersion.assertFromString(unversionLines(vLf, version))
            ),
            depends = unversionLines(versionedDepends, version).split('\n').toSeq.filter(_ != ""),
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

    private case class VersionedLine(
        line: String,
        versions: Option[Seq[Int]],
    )

    private def readVersionedLines(contents: String): Seq[VersionedLine] = {
      val versionedLinePat: Regex = " *-- @V(.*)$".r
      val intPat: Regex = "\\d+".r
      contents.split('\n').toSeq.map { line =>
        versionedLinePat
          .findFirstMatchIn(line)
          .map { m =>
            VersionedLine(
              line = line.take(m.start),
              versions = Some(intPat.findAllMatchIn(m.group(1)).toSeq.map(_.group(0).toInt)),
            )
          }
          .getOrElse(VersionedLine(line, None))
      }
    }

    private def getVersionedModule(
        packageName: String,
        lines: Seq[VersionedLine],
        version: Int,
    ): (String, String) = {
      val modNamePat: Regex = "module +([^ ]+) +where".r
      val contents = unversionLines(lines, version)
      modNamePat.findFirstMatchIn(contents) match {
        case Some(m) => (m.group(1), contents)
        case None =>
          throw BuildError(
            s"Failed to extract module name for a MODULE with package = ${packageName}"
          )
      }
    }

    private def unversionLines(
        lines: Seq[VersionedLine],
        version: Int,
    ): String =
      lines
        .collect { case vl if vl.versions.fold(true)(_.contains(version)) => vl.line }
        .mkString("\n")

    private def assertUnique(
        packageName: String,
        modName: String,
        modDefs: Seq[String],
    ): (String, String) = {
      modDefs match {
        case Seq(modDef) => (modName, modDef)
        case _ =>
          throw BuildError(
            s"Multiple conflicting definitions of module ${modName} in package ${packageName}"
          )
      }
    }
  }
}

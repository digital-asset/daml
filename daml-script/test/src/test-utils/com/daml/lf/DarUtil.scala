// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageVersion
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.sys.process._

object DarUtil {
  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  private val damlc = requiredResource(s"compiler/damlc/damlc$exe")

  def dummy(): Unit = {
    print(s"${exe}, ${damlc}")
  }

  case class DataDep(
      path: Path,
      prefix: Option[(String, String)] = None,
  )

  case class Dar(
      versionedName: String,
      version: Int,
      path: Path,
      mainPackageId: PackageId,
  )

  def buildDar(
      name: String,
      version: Int = 1,
      lfVersion: LanguageVersion,
      modules: Map[String, String],
      deps: Seq[Path] = Seq.empty,
      dataDeps: Seq[DataDep] = Seq.empty,
      opts: Seq[String] = Seq(),
      logger: ProcessLogger = ProcessLogger(_ => (), _ => ()),
      tmpDir: Option[Path] = None,
  ): Either[Int, Dar] = {

    def writeDamlYaml(pkgRoot: Path) = {
      val fileContent =
        s"""sdk-version: 0.0.0
          |build-options: [--target=${lfVersion.pretty}]
          |name: $name
          |source: .
          |version: $version.0.0
          |dependencies:
          |  - daml-prim
          |  - daml-stdlib
          |${deps.map(d => "  - " + d.toString).mkString("\n")}
          |data-dependencies:
          |${dataDeps.map(d => "  - " + d.path.toString).mkString("\n")}
          |module-prefixes:
          |${dataDeps
            .collect { d =>
              d.prefix match {
                case Some(prefix) => s"  ${prefix._1}: ${prefix._2}"
              }
            }
            .mkString("\n")}
          |""".stripMargin
      Files.write(pkgRoot.resolve("daml.yaml"), fileContent.getBytes(StandardCharsets.UTF_8))
    }

    def writeModule(pkgRoot: Path, modFullName: String, fileContent: String) = {
      val path = pkgRoot.resolve(Paths.get(modFullName.replace(".", "/") + ".daml"))
      val _ = Files.createDirectories(path.getParent())
      Files.write(path, fileContent.getBytes(StandardCharsets.UTF_8))
    }

    def damlBuild(pkgRoot: Path): Int = {
      (Seq(damlc.toString, "build", "--project-root", pkgRoot.toString) ++ opts).!(logger)
    }

    val versionedName = s"${name}-${version}.0.0"
    val pkgRoot = Files.createDirectory(
      tmpDir.getOrElse(Files.createTempDirectory("dar-util")).resolve(versionedName)
    )
    val _ = writeDamlYaml(pkgRoot)
    modules.foreachEntry(writeModule(pkgRoot, _, _))
    val exitCode = damlBuild(pkgRoot)
    if (exitCode == 0) {
      val darPath = pkgRoot.resolve(s".daml/dist/${versionedName}.dar")
      Right(
        Dar(
          versionedName,
          version,
          path = darPath,
          mainPackageId = PackageId.assertFromString(
            DarParser.assertReadArchiveFromFile(darPath.toFile).main.getHash
          ),
        )
      )
    } else {
      Left(exitCode)
    }
  }
}

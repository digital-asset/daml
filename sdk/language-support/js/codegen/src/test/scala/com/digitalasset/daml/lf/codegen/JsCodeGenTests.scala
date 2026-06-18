// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.nio.file.Path
import java.nio.file.Files
import java.util.Comparator
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Success
import scala.sys.process.{Process, ProcessLogger}

import com.daml.bazeltools.BazelRunfiles._
import org.scalactic.source.Position
import org.scalatest.{Assertion, Assertions}
import org.scalatest.wordspec.AsyncWordSpec

final class JsCodeGenTests extends AsyncWordSpec with Assertions {
  private val damlc = Path.of(rlocation(s"compiler/damlc/damlc$exe"))

  "JsCodeGen" should {
    "fail to generate packages with same name and version" in {
      withTestDir { implicit testDir =>
        val groverDar = buildDar(
          name = "grover",
          Seq("Grover.daml" -> "module Grover where data Grover = Grover"),
        )
        val anotherGroverDar = buildDar(
          name = "grover", // same name as package name as groverDar
          Seq("Elmo.daml" -> "module Elmo where data Elmo = Elmo"),
        )
        for (failure <- runGen(groverDar, anotherGroverDar).failed) yield {
          val message = failure.getMessage
          assert(message.contains("Duplicate name 'grover-1.0.0' for different packages detected"))
        }
      }
    }

    "accept twice the same package" in {
      withTestDir { implicit testDir =>
        val groverDar = buildDar(
          name = "grover",
          Seq("Grover.daml" -> "module Grover where data Grover = Grover"),
        )
        for (outputDir <- runGen(groverDar, groverDar))
          yield assertTsFilesExist(outputDir)("grover-1.0.0/lib/index")
      }
    }

    "generate the index tree" in {
      withTestDir { implicit testDir =>
        val testDar = buildDar(
          name = "test",
          Seq(
            "A.daml" -> "module A where data X = X",
            "A/B/C.daml" -> "module A.B.C where data Y = Y",
            "A/B/D.daml" -> "module A.B.D where data Z = Z",
          ),
        )
        for (outputDir <- runGen(testDar)) yield {
          val libDir = outputDir.resolve("test-1.0.0/lib")
          assertTsFilesExist(libDir)(
            "index",
            "A/index",
            "A/module",
            "A/B/index",
            "A/B/C/index",
            "A/B/D/index",
            "A/B/D/module",
          )
          assertTsFilesDoNotExist(libDir)("A/B/module")

          def reexportIndex(name: String): Seq[String] =
            Seq(s"import * as $name from './$name';", s"export { $name };")

          val reexportModule = Seq("export * from './module';")

          assertFilesContain(libDir)(
            "index.d.ts" -> reexportIndex("A"),
            "A/index.d.ts" -> (reexportIndex("B") ++ reexportModule),
            "A/B/index.d.ts" -> (reexportIndex("C") ++ reexportIndex("D")),
            "A/B/C/index.d.ts" -> reexportModule,
            "A/B/D/index.d.ts" -> reexportModule,
          )
        }
      }
    }

  }

  private def assertTsFilesExist(dir: Path)(fileNames: String*)(implicit
      pos: Position
  ): Assertion =
    fileNames.foldLeft(succeed) { (_, fileName) =>
      assert(Files.exists(dir.resolve(s"$fileName.js")), s"missing file: $fileName.js")
      assert(Files.exists(dir.resolve(s"$fileName.d.ts")), s"missing file: $fileName.d.ts")
    }

  private def assertTsFilesDoNotExist(dir: Path)(fileNames: String*)(implicit
      pos: Position
  ): Assertion =
    fileNames.foldLeft(succeed) { (_, fileName) =>
      assert(!Files.exists(dir.resolve(s"$fileName.js")), s"$fileName.js should not exist")
      assert(!Files.exists(dir.resolve(s"$fileName.d.ts")), s"$fileName.d.ts should not exist")
    }

  private def assertFilesContain(dir: Path)(fileAndLines: (String, Seq[String])*)(implicit
      pos: Position
  ): Assertion =
    fileAndLines.foldLeft(succeed) { case (_, (fileName, lines)) =>
      val content = Files.readString(dir.resolve(fileName))
      lines.foldLeft(succeed)((_, line) => assert(content.contains(line)))
    }

  private def runGen(dars: Path*)(implicit dir: TestDir): Future[Path] = {
    val outputDir = dir / "out"
    JsCodeGen.run(dars, outputDir, npmScope = "daml2js", damlVersion = "0.0.0").map(_ => outputDir)
  }

  private def buildDar(name: String, sources: Seq[(String, String)])(implicit
      dir: TestDir
  ): Path = {
    val packageDir = if (!Files.exists(dir / name)) dir / name else dir / s"$name-bis"
    Files.createDirectories(packageDir)
    Files.write(
      packageDir.resolve("daml.yaml"),
      s"""|name: $name
          |version: 1.0.0
          |source: .
          |dependencies:
          |  - daml-prim
          |  - daml-stdlib
          |""".stripMargin.getBytes,
    )
    sources.foreach { case (file, content) =>
      val path = packageDir.resolve(file)
      Files.createDirectories(path.getParent)
      Files.write(path, content.getBytes)
    }
    execSilently(damlc, Seq("build"), workingDir = packageDir)
    packageDir.resolve(s".daml/dist/$name-1.0.0.dar")
  }

  private class TestDir(val path: Path) {
    def /(file: String): Path = path.resolve(file)
  }

  private def withTestDir[A](f: TestDir => Future[A]): Future[A] = {
    val tempDir = Files.createTempDirectory("ts-codegen-test")
    f(new TestDir(tempDir)).andThen {
      case Success(_) => Files.walk(tempDir).sorted(Comparator.reverseOrder).forEach(Files.delete)
      case _ =>
        // keep directory in case of failure
        println(s"test directory is: $tempDir")
    }
  }

  private def execSilently(cmd: Path, args: Seq[String], workingDir: Path): Unit = {
    val output = mutable.Buffer.empty[String]
    val logger = ProcessLogger(output.append, output.append)
    val exitCode = Process(cmd.toString +: args, cwd = Some(workingDir.toFile)).!(logger)
    if (exitCode != 0) {
      fail(
        s"""|command failed: ${cmd.getFileName} ${args.mkString(" ")}
            |  ${output.mkString(s"\n  ")}""".stripMargin
      )
    }
  }
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import java.nio.file.{Files, Path, Paths}
import java.io.File
import java.util.stream.Collectors
import scalaz.syntax.traverse._
import spray.json._

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}

import com.digitalasset.daml.lf.engine.script._

object JsonApi {

  case class TestBasic(dar: Dar[(PackageId, Package)], runner: JsonTestRunner) {
    val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:jsonBasic"))
    def runTests(): Unit = {
      runner.genericTest(
        "basic",
        scriptId,
        Some(JsString("Alice")), {
          case v => TestRunner.assertEqual(v, SInt64(42), "exercise result")
        }
      )
    }
  }

  case class TestCreateAndExercise(dar: Dar[(PackageId, Package)], runner: JsonTestRunner) {
    val scriptId =
      Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:jsonCreateAndExercise"))
    def runTests(): Unit = {
      runner.genericTest(
        "createAndExercise",
        scriptId,
        Some(JsString("Alice")), {
          case v => TestRunner.assertEqual(v, SInt64(42), "exercise result")
        }
      )
    }
  }

  case class TestExerciseByKey(dar: Dar[(PackageId, Package)], runner: JsonTestRunner) {
    val scriptId =
      Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:jsonExerciseByKey"))
    def runTests(): Unit = {
      runner.genericTest(
        "exerciseByKey",
        scriptId,
        Some(JsString("Alice")), {
          case SRecord(_, _, vals) if vals.size == 2 =>
            TestRunner.assertEqual(vals.get(0), vals.get(1), "contract ids")
          case v => Left(s"Expected Tuple2 but got $v")
        }
      )
    }
  }

  case class Config(
      darPath: File,
      accessTokenFile: Path,
  )

  private val configParser = new scopt.OptionParser[Config]("daml_script_test") {
    head("daml_script_test")

    arg[File]("<dar>")
      .required()
      .action((d, c) => c.copy(darPath = d))

    opt[String]("access-token-file")
      .required()
      .action { (f, c) =>
        c.copy(accessTokenFile = Paths.get(f))
      }
  }

  private val applicationId = ApplicationId("DAML Script Tests")

  def main(args: Array[String]): Unit = {
    configParser.parse(args, Config(null, null)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val participantParams =
          Participants(Some(ApiParameters("http://localhost", 7500)), Map.empty, Map.empty)

        val token =
          Files.readAllLines(config.accessTokenFile).stream.collect(Collectors.joining("\n"))

        val runner =
          new JsonTestRunner(
            participantParams,
            dar,
            token
          )

        TestBasic(dar, runner).runTests()
        TestExerciseByKey(dar, runner).runTests()
        TestCreateAndExercise(dar, runner).runTests()
    }
  }
}

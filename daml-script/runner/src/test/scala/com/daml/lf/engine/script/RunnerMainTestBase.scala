// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.scalautil.Statement.discard
import java.nio.file.{Path, Paths}
import org.scalatest.compatible.Assertion
import org.scalatest.Suite
import scala.concurrent.{Future, ExecutionContext}
import scala.sys.process._

trait RunnerMainTestBase {
  self: Suite =>
  protected val jwt: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/src/test/resources/json-access.jwt"))
  protected val inputFile: String =
    BazelRunfiles.rlocation("daml-script/runner/src/test/resources/input.json")

  // Defines the size of `dars`
  // Should always match test_dar_count in the BUILD file
  val DAR_COUNT = 5

  // We use a different DAR for each test that asserts upload behaviour to avoid clashes
  val dars: Seq[Path] = (1 to DAR_COUNT).map(n =>
    BazelRunfiles.rlocation(Paths.get(s"daml-script/runner/test-script$n.dar"))
  )

  // DAR containing failingScript and successfulScript
  val failingDar: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/failing-test-script.dar"))

  implicit val ec = ExecutionContext.global

  val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  val damlScript = BazelRunfiles.rlocation(Paths.get(s"daml-script/runner/daml-script-binary$exe"))

  // Runs process with args, returns status and stdout <> stderr
  private def runProc(exe: Path, args: Seq[String]): Future[Either[String, String]] =
    Future {
      val out = new StringBuilder()
      val cmd = exe.toString +: args
      cmd !< ProcessLogger(line => discard(out append line)) match {
        case 0 => Right(out.toString)
        case _ => Left(out.toString)
      }
    }

  def testDamlScript(
      dar: Path,
      args: Seq[String],
      expectedResult: Either[Seq[String], Seq[String]] = Right(Seq()),
  ): Future[Assertion] =
    runProc(damlScript, Seq("--dar", dar.toString) ++ args).map { res =>
      (res, expectedResult) match {
        case (Right(actual), Right(expecteds)) =>
          if (expecteds.forall(actual contains _)) succeed
          else
            fail(
              s"Expected daml-script output to contain '${expecteds.mkString("', '")}' but it did not:\n$actual"
            )

        case (Left(actual), Left(expecteds)) =>
          if (expecteds.forall(actual contains _)) succeed
          else
            fail(
              s"Expected daml-script output to contain '${expecteds.mkString("', '")}' but it did not:\n$actual"
            )

        case (Right(_), Left(expecteds)) =>
          fail(s"Expected daml-script to fail with ${expecteds.mkString("', '")} but it succeeded.")

        case (Left(actual), Right(_)) =>
          fail(s"Expected daml-script to succeed but it failed with $actual")
      }
    }
}

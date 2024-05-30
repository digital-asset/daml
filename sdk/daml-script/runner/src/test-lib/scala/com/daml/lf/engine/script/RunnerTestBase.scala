// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.scalautil.Statement.discard
import java.nio.file.{Path, Paths}
import org.scalatest.compatible.Assertion
import org.scalatest.Suite
import scala.concurrent.{Future, ExecutionContext}
import scala.sys.process._

trait RunnerTestBase {
  self: Suite =>

  private implicit val ec: ExecutionContext = ExecutionContext.global

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
    testDamlScriptPred(dar, args) { res =>
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

  def testDamlScriptPred(
      dar: Path,
      args: Seq[String],
  )(resultAssertion: Either[String, String] => Assertion): Future[Assertion] =
    runProc(damlScript, Seq("--dar", dar.toString) ++ args).map(resultAssertion(_))
}

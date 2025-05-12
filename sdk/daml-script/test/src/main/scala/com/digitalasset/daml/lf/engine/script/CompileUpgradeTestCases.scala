// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.UpgradeTestUtil
import com.digitalasset.daml.lf.UpgradeTestUtil.TestCase
import com.digitalasset.daml.lf.language.LanguageVersion

import java.nio.file.{Path, Paths}
import scala.concurrent.Await
import scala.util.matching.Regex

object CompileUpgradeTestCases {

  private abstract class Command

  private case object ListTests extends Command

  private case class CompileTests(regex: Option[Regex]) extends Command

  private def parseArgs(args: Array[String]): Command = {
    args match {
      case Array("list") => ListTests
      case Array("compile") => CompileTests(None)
      case Array("compile", regex) => CompileTests(Some(new Regex(regex)))
      case _ =>
        println("Usage: compile-upgrade-test-cases (list|compile [<regex>])")
        sys.exit(0)
    }
  }

  private case class Target(
      languageVersion: LanguageVersion,
      upgradeTestLibDarPath: Path,
      testFilesDir: Path,
  )

  private val targets: Seq[Target] = Seq(
    Target(
      LanguageVersion.Major.V2.maxStableVersion,
      rlocation(Paths.get("daml-script/test/upgrade-test-lib.dar")),
      rlocation(Paths.get("daml-script/test/daml/upgrades/stable")),
    ),
    Target(
      LanguageVersion.Major.V2.dev,
      rlocation(Paths.get("daml-script/test/upgrade-test-lib-dev.dar")),
      rlocation(Paths.get("daml-script/test/daml/upgrades/dev")),
    ),
  )

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    for (target <- targets) {
      val testUtil = new UpgradeTestUtil(target.upgradeTestLibDarPath)
      lazy val testCases: Seq[TestCase] =
        UpgradeTestUtil.getTestCases(target.languageVersion, target.testFilesDir)

      parseArgs(args) match {
        case ListTests => testCases.foreach(testCase => println(testCase.name))
        case CompileTests(maybeRegex) =>
          val filteredTestCases = maybeRegex match {
            case Some(regex) =>
              testCases.filter(testCase => regex.unanchored.matches(testCase.name))
            case None => testCases
          }
          filteredTestCases.foreach { testCase =>
            println(s"compiling ${testCase.name}")
            Await.result(
              testUtil.buildTestCaseDarMemoized(target.languageVersion, testCase),
              scala.concurrent.duration.Duration.Inf,
            )
          }
      }
    }
  }
}

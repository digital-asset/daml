// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import com.daml.ledger.api.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerSession,
  LedgerSessionConfiguration,
  LedgerTestSuite,
  LedgerTestSuiteRunner,
  LedgerTestSummary
}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object LedgerApiTestTool {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // The suffix that will be appended to all party and command identifiers to ensure
  // they are unique across test runs (but still somewhat stable within a single test run)
  // This implementation could fail based on the limitations of System.nanoTime, that you
  // can read on here: https://docs.oracle.com/javase/8/docs/api/java/lang/System.html#nanoTime--
  // Still, the only way in which this can fail is if two test runs target the same ledger
  // with the identifier suffix being computed to the same value, which at the very least
  // requires this to happen on what is resolved by the JVM as the very same millisecond.
  // This is very unlikely to fail and allows to easily "date" parties on a ledger used
  // for testing and compare data related to subsequent runs without any reference
  private[this] val identifierSuffix = f"${System.nanoTime}%x"

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION ON MAIN THREAD, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private def exitCode(summaries: Vector[LedgerTestSummary], expectFailure: Boolean): Int =
    if (summaries.exists(_.result.isLeft) == expectFailure) 0 else 1

  private def printAvailableTests(): Unit = {
    println("Tests marked with * are run by default.")
    println(
      "You can include extra tests with `--include=TEST-NAME`, or run all tests with `--all-tests`.\n")
    Tests.default.keySet.toSeq.sorted.map(_ + " *").foreach(println(_))
    Tests.optional.keySet.toSeq.sorted.foreach(println(_))

    println("\nAlternatively, you can run performance tests.")
    println(
      "Performance tests are not run by default, but can be run with `--perf-tests=TEST-NAME`.\n")
    Tests.PerformanceTestsKeys.sorted.foreach(println(_))
  }

  private def extractResources(resources: String*): Unit = {
    val pwd = Paths.get(".").toAbsolutePath
    println(s"Extracting all DAML resources necessary to run the tests into $pwd.")
    for (resource <- resources) {
      val is = getClass.getResourceAsStream(resource)
      if (is == null) sys.error(s"Could not find $resource in classpath")
      val targetFile = new File(new File(resource).getName)
      Files.copy(is, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      println(s"Extracted $resource to $targetFile")
    }
  }

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    if (config.listTests) {
      printAvailableTests()
      sys.exit(0)
    }

    if (config.extract) {
      // This must be kept aligned manually with artifacts declared in /ledger/test-common/BUILD.bazel.
      extractResources(
        "/ledger/test-common/SemanticTests.dar",
        "/ledger/test-common/Test-stable.dar",
        "/ledger/test-common/Test-dev.dar",
        "/ledger/test-common/PerformanceTests.dar",
      )
      sys.exit(0)
    }

    if (config.participants.isEmpty) {
      println("No participant to test, exiting.")
      sys.exit(0)
    }

    val missingTests = (config.included ++ config.excluded).filterNot(Tests.all.contains)
    if (missingTests.nonEmpty) {
      println("The following tests could not be found:")
      missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    val included =
      if (config.allTests) Tests.all.keySet
      else if (config.included.isEmpty) Tests.default.keySet
      else config.included

    val testsToRun = Tests.all.filterKeys(included -- config.excluded)
    val performanceTestsToRun =
      Tests.performanceTests(config.performanceTestsReport).filterKeys(config.performanceTests)

    if (testsToRun.isEmpty && performanceTestsToRun.isEmpty) {
      println("No tests to run.")
      sys.exit(0)
    } else if ((config.allTests || config.included.nonEmpty) && performanceTestsToRun.nonEmpty) {
      println("Either regular or performance tests can be run, but not both.")
      sys.exit(64)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      })

    val runner =
      if (performanceTestsToRun.nonEmpty)
        newLedgerSuiteRunner(
          config,
          performanceTestsToRun.values,
          concurrencyOverride = Some(1),
        )
      else
        newLedgerSuiteRunner(
          config,
          testsToRun.values,
        )

    runner.verifyRequirementsAndRun {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(System.out, config.verbose).report(summaries)
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(e) =>
        logger.error(e.getMessage, e)
        sys.exit(1)
    }
  }

  private[this] def newLedgerSuiteRunner(
      config: Config,
      suites: Iterable[LedgerSession => LedgerTestSuite],
      concurrencyOverride: Option[Int] = None): LedgerTestSuiteRunner =
    new LedgerTestSuiteRunner(
      LedgerSessionConfiguration(
        config.participants,
        config.shuffleParticipants,
        config.tlsConfig,
        config.loadScaleFactor,
        config.partyAllocation,
      ),
      suites.toVector,
      identifierSuffix,
      config.timeoutScaleFactor,
      concurrencyOverride.getOrElse(config.concurrentTestRuns),
    )
}

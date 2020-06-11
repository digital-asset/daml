// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import com.daml.ledger.api.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerSessionConfiguration,
  LedgerTestSuite,
  LedgerTestSuiteRunner,
  LedgerTestSummary
}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Random, Success}

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

  private def print[A](defaults: Seq[A], optionals: Seq[A])(t: A => String): Unit = {
    println("Tests marked with * are run by default.")
    println(
      "You can include extra tests with `--include=TEST-NAME`, or run all tests with `--all-tests`.\n")
    defaults.map(t(_) + " * ").sorted.foreach(println(_))
    optionals.map(t).sorted.foreach(println(_))

    println("\nAlternatively, you can run performance tests.")
    println(
      "Performance tests are not run by default, but can be run with `--perf-tests=TEST-NAME`.\n")
    Tests.PerformanceTestsKeys.sorted.foreach(println(_))
  }
  private def printAvailableTestSuites(config: Config): Unit = {
    println("Listing test suites. Run with --list-all to see individual tests.")
    print(Tests.default, Tests.optional(config))(_.name)
  }

  private def printAvailableTests(config: Config): Unit = {
    println("Listing all tests. Run with --list to only see test suites.")
    print(Tests.default.flatMap(_.tests), Tests.optional(config).flatMap(_.tests))(_.name)
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

    if (config.listTestSuites) {
      printAvailableTestSuites(config)
      sys.exit(0)
    }

    if (config.listTests) {
      printAvailableTests(config)
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

    val missingTests = (config.included ++ config.excluded).filterNot(pref =>
      Tests.all(config).flatMap(_.tests).map(_.name).exists(test => test.startsWith(pref)))
    if (missingTests.nonEmpty) {
      println("The following exclusion or inclusion does not match any test:")
      missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    val performanceTestsToRun =
      Tests
        .performanceTests(config.performanceTestsReport)
        .filter(suite => config.performanceTests(suite.name))

    if ((config.allTests || config.included.nonEmpty) && performanceTestsToRun.nonEmpty) {
      println("Either regular or performance tests can be run, but not both.")
      sys.exit(64)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      })

    val included_p =
      if (config.allTests) Tests.all(config).map(_.name).toSet
      else if (config.included.isEmpty) Tests.default.map(_.name).toSet
      else config.included
    val shouldRun: String => Boolean = caseName => {
      included_p.exists(caseName.startsWith) && !config.excluded.exists(caseName.startsWith)
    }

    val runner =
      if (performanceTestsToRun.nonEmpty)
        newLedgerSuiteRunner(
          config,
          performanceTestsToRun,
          caseName => true,
          concurrencyOverride = Some(1),
        )
      else
        newLedgerSuiteRunner(
          config,
          Random.shuffle(Tests.all(config)),
          shouldRun,
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
      suites: Iterable[LedgerTestSuite],
      shouldRun: String => Boolean,
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
      shouldRun,
      concurrencyOverride.getOrElse(config.concurrentTestRuns),
    )
}

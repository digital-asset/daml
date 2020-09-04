// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import com.daml.ledger.api.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.testtool.infrastructure.{
  Dars,
  LedgerSessionConfiguration,
  LedgerTestCase,
  LedgerTestCasesRunner,
  LedgerTestSuite,
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

  private def printListOfTests[A](tests: Seq[A])(getName: A => String): Unit = {
    println("All tests are run by default.")
    println()
    tests.map(getName).sorted.foreach(println(_))

    println()
    println("Alternatively, you can run performance tests.")
    println("They are not run by default, but can be run with `--perf-tests=TEST-NAME`.")
    println()
    Tests.PerformanceTestsKeys.foreach(println(_))
  }
  private def printAvailableTestSuites(testSuites: Vector[LedgerTestSuite]): Unit = {
    println("Listing test suites. Run with --list-all to see individual tests.")
    printListOfTests(testSuites)(_.name)
  }

  private def printAvailableTests(testSuites: Vector[LedgerTestSuite]): Unit = {
    println("Listing all tests. Run with --list to only see test suites.")
    printListOfTests(testSuites.flatMap(_.tests))(_.name)
  }

  private def extractResources(resources: Seq[String]): Unit = {
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

  private def cases(m: Map[String, LedgerTestSuite]): Iterable[LedgerTestCase] =
    m.values.flatMap(_.tests)

  private def matches(prefixes: Iterable[String])(test: LedgerTestCase): Boolean =
    prefixes.exists(test.name.startsWith)

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    val allTests: Vector[LedgerTestSuite] = Tests.all(config)
    val allTestCaseNames: Set[String] =
      (allTests ++ Tests.retired).flatMap(_.tests).map(_.name).toSet
    val missingTests = (config.included ++ config.excluded).filterNot(prefix =>
      allTestCaseNames.exists(_.startsWith(prefix)))
    if (missingTests.nonEmpty) {
      println("The following exclusion or inclusion does not match any test:")
      missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    if (config.listTestSuites) {
      printAvailableTestSuites(allTests)
      sys.exit(0)
    }

    if (config.listTests) {
      printAvailableTests(allTests)
      sys.exit(0)
    }

    if (config.extract) {
      extractResources(Dars.resources)
      sys.exit(0)
    }

    if (config.participants.isEmpty) {
      println("No participant to test, exiting.")
      sys.exit(0)
    }

    val performanceTestsToRun =
      Tests.performanceTests(config.performanceTestsReport).filterKeys(config.performanceTests)

    if (config.included.nonEmpty && performanceTestsToRun.nonEmpty) {
      println("Either regular or performance tests can be run, but not both.")
      sys.exit(64)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      })

    val allCases = allTests.flatMap(_.tests)
    val retiredCases = Tests.retired.flatMap(_.tests)

    val includedTests =
      if (config.included.isEmpty) allCases
      else (allCases ++ retiredCases).filter(matches(config.included))

    val testsToRun: Iterable[LedgerTestCase] =
      includedTests.filterNot(matches(config.excluded))

    val runner =
      if (performanceTestsToRun.nonEmpty)
        newLedgerCasesRunner(
          config,
          cases(performanceTestsToRun),
          concurrencyOverride = Some(1),
        )
      else
        newLedgerCasesRunner(
          config,
          testsToRun,
        )

    runner.verifyRequirementsAndRun {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(
          System.out,
          config.verbose,
        ).report(summaries)
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(e) =>
        logger.error(e.getMessage, e)
        sys.exit(1)
    }
  }

  private[this] def newLedgerCasesRunner(
      config: Config,
      cases: Iterable[LedgerTestCase],
      concurrencyOverride: Option[Int] = None): LedgerTestCasesRunner =
    new LedgerTestCasesRunner(
      LedgerSessionConfiguration(
        config.participants,
        config.shuffleParticipants,
        config.tlsConfig,
        config.partyAllocation,
      ),
      cases.toVector,
      identifierSuffix,
      config.timeoutScaleFactor,
      concurrencyOverride.getOrElse(config.concurrentTestRuns),
    )
}

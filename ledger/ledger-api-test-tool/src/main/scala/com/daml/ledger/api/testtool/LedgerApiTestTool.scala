// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.Executors

import com.daml.ledger.api.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.testtool.infrastructure._
import com.daml.ledger.api.testtool.tests.Tests
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.resources.{AbstractResourceOwner, Resource}
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
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

  private def cases(m: Map[String, LedgerTestSuite]): Vector[LedgerTestCase] =
    m.values.view.flatMap(_.tests).toVector

  private def matches(prefixes: Iterable[String])(test: LedgerTestCase): Boolean =
    prefixes.exists(test.name.startsWith)

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    val defaultTests: Vector[LedgerTestSuite] = Tests.default(config.ledgerClockGranularity)
    val visibleTests: Vector[LedgerTestSuite] = defaultTests ++ Tests.optional
    val allTests: Vector[LedgerTestSuite] = visibleTests ++ Tests.retired
    val allTestCaseNames: Set[String] = allTests.flatMap(_.tests).map(_.name).toSet
    val missingTests = (config.included ++ config.excluded).filterNot(prefix =>
      allTestCaseNames.exists(_.startsWith(prefix))
    )
    if (missingTests.nonEmpty) {
      println("The following exclusion or inclusion does not match any test:")
      missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    if (config.listTestSuites) {
      printAvailableTestSuites(visibleTests)
      sys.exit(0)
    }

    if (config.listTests) {
      printAvailableTests(visibleTests)
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

    val defaultCases = defaultTests.flatMap(_.tests)
    val allCases = defaultCases ++ Tests.optional.flatMap(_.tests) ++ Tests.retired.flatMap(_.tests)

    val includedTests =
      if (config.included.isEmpty) defaultCases
      else allCases.filter(matches(config.included))

    val testsToRun: Vector[LedgerTestCase] =
      includedTests.filterNot(matches(config.excluded))

    implicit val resourceManagementExecutionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

    val runner =
      if (performanceTestsToRun.nonEmpty)
        newSequentialLedgerCasesRunner(
          config,
          cases(performanceTestsToRun),
        )
      else
        newLedgerCasesRunner(
          config,
          testsToRun,
        )

    runner.flatMap(_.runTests).onComplete {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(
          System.out,
          config.verbose,
        ).report(summaries, identifierSuffix)
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(exception: Errors.FrameworkException) =>
        logger.error(exception.getMessage)
        logger.debug(exception.getMessage, exception)
        sys.exit(1)
      case Failure(exception) =>
        logger.error(exception.getMessage, exception)
        sys.exit(1)
    }
  }

  private[this] def newSequentialLedgerCasesRunner(
      config: Config,
      cases: Vector[LedgerTestCase],
  )(implicit executionContext: ExecutionContext): Future[LedgerTestCasesRunner] =
    createLedgerCasesRunner(config, cases, concurrentTestRuns = 1)

  private[this] def newLedgerCasesRunner(
      config: Config,
      cases: Vector[LedgerTestCase],
  )(implicit executionContext: ExecutionContext): Future[LedgerTestCasesRunner] =
    createLedgerCasesRunner(config, cases, config.concurrentTestRuns)

  private[this] def createLedgerCasesRunner(
      config: Config,
      cases: Vector[LedgerTestCase],
      concurrentTestRuns: Int,
  )(implicit executionContext: ExecutionContext): Future[LedgerTestCasesRunner] = {
    initializeParticipantChannels(config.participants, config.tlsConfig).asFuture.map(
      participants =>
        new LedgerTestCasesRunner(
          testCases = cases,
          participants = participants,
          partyAllocation = config.partyAllocation,
          shuffleParticipants = config.shuffleParticipants,
          timeoutScaleFactor = config.timeoutScaleFactor,
          concurrentTestRuns = concurrentTestRuns,
          uploadDars = config.uploadDars,
          identifierSuffix = identifierSuffix,
        )
    )
  }

  private def initializeParticipantChannel(
      host: String,
      port: Int,
      tlsConfig: Option[TlsConfiguration],
  ): AbstractResourceOwner[ExecutionContext, Channel] = {
    logger.info(s"Setting up managed channel to participant at $host:$port...")
    val channelBuilder = NettyChannelBuilder.forAddress(host, port).usePlaintext()
    for (ssl <- tlsConfig; sslContext <- ssl.client) {
      logger.info("Setting up managed channel with transport security.")
      channelBuilder
        .useTransportSecurity()
        .sslContext(sslContext)
        .negotiationType(NegotiationType.TLS)
    }
    channelBuilder.maxInboundMessageSize(10000000)
    ResourceOwner.forChannel(channelBuilder, shutdownTimeout = 5.seconds)
  }

  private def initializeParticipantChannels(
      participants: Vector[(String, Int)],
      tlsConfig: Option[TlsConfiguration],
  )(implicit executionContext: ExecutionContext): Resource[ExecutionContext, Vector[Channel]] = {
    val participantChannelOwners =
      for ((host, port) <- participants) yield {
        initializeParticipantChannel(host, port, tlsConfig)
      }
    Resource.sequence(participantChannelOwners.map(_.acquire()))
  }

}

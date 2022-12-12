// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.Executors

import com.daml.ledger.api.testtool.infrastructure._
import com.daml.ledger.api.testtool.runner.TestRunner._
import com.daml.ledger.api.tls.TlsConfiguration
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object TestRunner {

  private type ResourceOwner[T] = com.daml.resources.AbstractResourceOwner[ExecutionContext, T]
  private type Resource[T] = com.daml.resources.Resource[ExecutionContext, T]
  private val Resource = new com.daml.resources.ResourceFactories[ExecutionContext]

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // The suffix that will be appended to all party and command identifiers to ensure
  // they are unique across test runs (but still somewhat stable within a single test run)
  // This implementation could fail based on the limitations of System.nanoTime, that you
  // can read on here: https://docs.oracle.com/javase/8/docs/api/java/lang/System.html#nanoTime--
  // Still, the only way in which this can fail is if two test runs target the same ledger
  // with the identifier suffix being computed to the same value, which at the very least
  // requires this to happen on what is resolved by the JVM as the very same millisecond.
  // This is very unlikely to fail and allows to easily "date" parties on a ledger used
  // for testing and compare data related to subsequent runs without any reference
  private val identifierSuffix = f"${System.nanoTime}%x"

  private val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION ON MAIN THREAD, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private def exitCode(summaries: Vector[LedgerTestSummary], expectFailure: Boolean): Int =
    if (summaries.exists(_.result.isLeft) == expectFailure) 0 else 1

  private def printListOfTests[A](tests: Seq[A])(getName: A => String): Unit = {
    println("All tests are run by default.")
    println()
    tests.map(getName).sorted.foreach(println(_))
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
    println(s"Extracting all Daml resources necessary to run the tests into $pwd.")
    for (resource <- resources) {
      val is = getClass.getClassLoader.getResourceAsStream(resource)
      if (is == null) sys.error(s"Could not find $resource in classpath")
      val targetFile = new File(new File(resource).getName)
      Files.copy(is, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      println(s"Extracted $resource to $targetFile")
    }
  }

  private def matches(prefixes: Iterable[String])(test: LedgerTestCase): Boolean =
    prefixes.exists(test.name.startsWith)
}

final class TestRunner(availableTests: AvailableTests, config: Config) {
  def runAndExit(): Unit = {
    val tests = new ConfiguredTests(availableTests, config)

    if (tests.missingTests.nonEmpty) {
      println("The following exclusion or inclusion does not match any test:")
      tests.missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    if (config.listTestSuites) {
      printAvailableTestSuites(tests.allTests)
      sys.exit(0)
    }

    if (config.listTests) {
      printAvailableTests(tests.allTests)
      sys.exit(0)
    }

    if (config.extract) {
      extractResources(Dars.resources)
      sys.exit(0)
    }

    if (config.participantsEndpoints.isEmpty) {
      println("No participant to test, exiting.")
      sys.exit(0)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      })

    val includedTests =
      if (config.included.isEmpty) tests.defaultCases
      else tests.allCases.filter(matches(config.included))

    val addedTests = tests.allCases.filter(matches(config.additional))

    val (excludedTests, testsToRun) =
      (includedTests ++ addedTests).partition(matches(config.excluded))

    implicit val resourceManagementExecutionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

    val runner = newLedgerCasesRunner(config, testsToRun)
    runner.flatMap(_.runTests(ExecutionContext.global)).onComplete {
      case Success(summaries) =>
        val excludedTestSummaries =
          excludedTests.map { ledgerTestCase =>
            LedgerTestSummary(
              suite = ledgerTestCase.suite.name,
              name = ledgerTestCase.name,
              description = ledgerTestCase.description,
              result = Right(Result.Excluded("excluded test")),
            )
          }
        new Reporter.ColorizedPrintStreamReporter(
          System.out,
          config.verbose,
        ).report(
          summaries,
          excludedTestSummaries,
          Seq(
            "identifierSuffix" -> identifierSuffix,
            "concurrentTestRuns" -> config.concurrentTestRuns.toString,
            "timeoutScaleFactor" -> config.timeoutScaleFactor.toString,
          ),
        )
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

  private def newLedgerCasesRunner(
      config: Config,
      cases: Vector[LedgerTestCase],
  )(implicit executionContext: ExecutionContext): Future[LedgerTestCasesRunner] =
    createLedgerCasesRunner(config, cases, config.concurrentTestRuns)

  private def createLedgerCasesRunner(
      config: Config,
      cases: Vector[LedgerTestCase],
      concurrentTestRuns: Int,
  )(implicit executionContext: ExecutionContext): Future[LedgerTestCasesRunner] = {
    initializeParticipantChannels(
      participantEndpoints = config.participantsEndpoints,
      tlsConfig = config.tlsConfig,
    ).asFuture
      .map(participantChannels =>
        new LedgerTestCasesRunner(
          testCases = cases,
          participantChannels = participantChannels,
          maxConnectionAttempts = config.maxConnectionAttempts,
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
  ): ResourceOwner[Channel] = {
    logger.info(s"Setting up managed channel to participant at $host:$port...")
    val channelBuilder = NettyChannelBuilder.forAddress(host, port).usePlaintext()
    for (ssl <- tlsConfig; sslContext <- ssl.client()) {
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
      participantEndpoints: Vector[(String, Int)],
      tlsConfig: Option[TlsConfiguration],
  )(implicit executionContext: ExecutionContext): Resource[Vector[ChannelEndpoint]] =
    Resource.sequence(participantEndpoints.map { case (host, port) =>
      initializeParticipantChannel(host, port, tlsConfig)
        .acquire()
        .map(channel => ChannelEndpoint.forRemote(channel = channel, hostname = host, port = port))
    })

}

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.PlatformApplications.RemoteApiEndpoint
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.semantictest.SandboxSemanticTestsLfRunner
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.platform.testing.LedgerBackend
import com.digitalasset.platform.tests.integration.ledger.api.TransactionServiceIT
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Args, Suite}

import scala.util.control.NonFatal

object LedgerApiTestTool {
  val semanticTestsResource = "/ledger/ledger-api-integration-tests/SemanticTests.dar"
  val integrationTestResource = "/ledger/sandbox/Test.dar"
  val testResources = List(
    integrationTestResource,
    semanticTestsResource,
  )
  def main(args: Array[String]): Unit = {

    val toolConfig = Cli
      .parse(args)
      .getOrElse(sys.exit(1))

    if (toolConfig.extract) {
      extractTestFiles(testResources)
      sys.exit(0)
    }

    val commonConfig = PlatformApplications.Config.default
      .withTimeProvider(TimeProviderType.WallClock)
      .withLedgerIdMode(LedgerIdMode.Dynamic())
      .withRemoteApiEndpoint(
        RemoteApiEndpoint.default
          .withHost(toolConfig.host)
          .withPort(toolConfig.port)
          .withTlsConfig(toolConfig.tlsConfig))

    val suites = suiteDefinitions(commonConfig)

    if (toolConfig.listTests) {
      println(s"The following tests are available: \n${suites.keySet.toList.sorted.mkString("\n")}")
      sys.exit(0)
    }

    var failed = false

    try {

      val suitesToRun = (if (toolConfig.included.isEmpty) suites.keySet else toolConfig.included)
        .filterNot(toolConfig.excluded)

      if (suitesToRun.isEmpty) {
        println("No tests to run.")
        sys.exit(0)
      }

      val reporter = new ToolReporter(toolConfig.verbose)
      val sorter = new ToolSorter

      suitesToRun
        .find(n => !suites.contains(n))
        .foreach { unknownSuite =>
          println(s"Unknown Test: $unknownSuite")
          sys.exit(-1)
        }

      suitesToRun.foreach { suiteName =>
        try {
          suites(suiteName)()
            .run(None, Args(reporter = reporter, distributedTestSorter = Some(sorter)))
        } catch {
          case NonFatal(t) =>
            println(s"Error while executing test [$suiteName]: ${t.getMessage}")
        }
        ()
      }
      reporter.printStatistics
    } catch {
      case NonFatal(t) =>
        failed = true
        if (!toolConfig.mustFail) throw t
    }

    if (toolConfig.mustFail) {
      if (failed) println("One or more scenarios failed as expected.")
      else
        throw new RuntimeException(
          "None of the scenarios failed, yet the --must-fail flag was specified!")
    }
  }

  private def suiteDefinitions(
      commonConfig: PlatformApplications.Config): Map[String, () => Suite] = {
    val semanticTestsRunner = lazyInit(
      "SemanticTests",
      name =>
        new SandboxSemanticTestsLfRunner {
          override def suiteName: String = name
          override def actorSystemName = "SandboxSemanticTestsLfRunnerTestToolActorSystem"

          override def fixtureIdsEnabled: Set[LedgerBackend] = Set(LedgerBackend.RemoteApiProxy)
          override implicit lazy val patienceConfig: PatienceConfig =
            PatienceConfig(Span(60L, Seconds))

          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(semanticTestsResource))
      }
    )

    val transactionServiceIT = lazyInit(
      "TransactionServiceTests",
      name =>
        new TransactionServiceIT {
          override def suiteName: String = name
          override def actorSystemName = "TransactionServiceITTestToolActorSystem"
          override def fixtureIdsEnabled: Set[LedgerBackend] = Set(LedgerBackend.RemoteApiProxy)

          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    Map(semanticTestsRunner, transactionServiceIT)
  }

  def lazyInit[A](name: String, factory: String => A): (String, () => A) = {
    (name, () => factory(name))
  }

  private def resourceAsFile(testResource: String): Path = {
    val integrationTestResourceStream =
      Option(getClass.getResourceAsStream(testResource))
    require(
      integrationTestResourceStream.isDefined,
      "Unable to load the required test DAR from resources.")
    val targetPath: Path = Files.createTempFile("ledger-api-test-tool-", "-test.dar")
    Files.copy(integrationTestResourceStream.get, targetPath, StandardCopyOption.REPLACE_EXISTING);
    targetPath
  }

  private def extractTestFiles(testResources: List[String]): Unit = {
    val pwd = Paths.get(".").toAbsolutePath
    println(s"Extracting all DAML resources necessary to run the tests into $pwd.")
    testResources
      .foreach { n =>
        val is = getClass.getResourceAsStream(n)
        if (is == null) sys.error(s"Could not find $n in classpath")
        val targetFile = new File(new File(n).getName)
        Files.copy(is, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
        println(s"Extracted $n to $targetFile")
      }
  }
}

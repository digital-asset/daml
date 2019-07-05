// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.digitalasset.platform.{PlatformApplications, RemoteApiEndpointMode}
import com.digitalasset.platform.PlatformApplications.RemoteApiEndpoint
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.semantictest.SandboxSemanticTestsLfRunner
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.platform.testing.LedgerBackend
import com.digitalasset.platform.tests.integration.ledger.api.commands.{
  CommandTransactionChecksHighLevelIT,
  CommandTransactionChecksLowLevelIT
}
import com.digitalasset.platform.tests.integration.ledger.api.{
  DivulgenceIT,
  PackageManagementServiceIT,
  PartyManagementServiceIT,
  TransactionServiceIT
}
import com.digitalasset.platform.tests.integration.ledger.api.transaction.TransactionBackpressureIT
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

    val (endpoint, backends): (RemoteApiEndpointMode, Set[LedgerBackend]) =
      toolConfig.mapping.isEmpty match {
        case true =>
          (
            RemoteApiEndpointMode.Single(
              RemoteApiEndpoint.default
                .withHost(toolConfig.host)
                .withPort(toolConfig.port)
                .withTlsConfig(toolConfig.tlsConfig)),
            Set(LedgerBackend.RemoteSingleApiProxy))
        case false =>
          val mapping: Map[Option[String], RemoteApiEndpoint] = toolConfig.mapping.map({
            case (party, (host, port)) =>
              Some(party) -> RemoteApiEndpoint.default
                .withHost(host)
                .withPort(port)
                .withTlsConfig(None)
          })
          (
            RemoteApiEndpointMode.MultiFromMapping(
              mapping + (None -> RemoteApiEndpoint.default
                .withHost(toolConfig.host)
                .withPort(toolConfig.port))),
            Set(LedgerBackend.RemoteMultiApiProxy))
      }

    val commonConfig = PlatformApplications.Config.default
      .withTimeProvider(TimeProviderType.WallClock)
      .withLedgerIdMode(LedgerIdMode.Dynamic())
      .withCommandSubmissionTtlScaleFactor(toolConfig.commandSubmissionTtlScaleFactor)
      .withUniquePartyIdentifiers(toolConfig.uniquePartyIdentifiers)
      .withUniqueCommandIdentifiers(toolConfig.uniqueCommandIdentifiers)
      .withRemoteApiEndpoint(endpoint)

    val default = defaultTests(commonConfig, toolConfig, backends)
    val optional = optionalTests(commonConfig, toolConfig, backends)

    val allTests = default ++ optional

    if (toolConfig.listTests) {
      println("Tests marked with * are run by default.\n")
      println(default.keySet.toSeq.sorted.map(_ + " *").mkString("\n"))
      println(optional.keySet.toSeq.sorted.mkString("\n"))
      sys.exit(0)
    }

    var failed = false

    try {

      val includedTests =
        if (toolConfig.allTests) allTests.keySet
        else if (toolConfig.included.isEmpty) default.keySet
        else toolConfig.included

      val testsToRun = includedTests.filterNot(toolConfig.excluded)

      if (testsToRun.isEmpty) {
        println("No tests to run.")
        sys.exit(0)
      }

      val reporter = new ToolReporter(toolConfig.verbose)
      val sorter = new ToolSorter

      testsToRun
        .find(n => !allTests.contains(n))
        .foreach { unknownSuite =>
          println(s"Unknown Test: $unknownSuite")
          sys.exit(1)
        }

      testsToRun.foreach { suiteName =>
        try {
          allTests(suiteName)()
            .run(None, Args(reporter = reporter, distributedTestSorter = Some(sorter)))
        } catch {
          case NonFatal(t) =>
            failed = true
        }
        ()
      }

      failed |= reporter.statistics.testsStarted != reporter.statistics.testsSucceeded
      reporter.printStatistics
    } catch {
      case NonFatal(t) if toolConfig.mustFail =>
        failed = true
    }

    if (toolConfig.mustFail) {
      if (failed) println("One or more scenarios failed as expected.")
      else
        throw new RuntimeException(
          "None of the scenarios failed, yet the --must-fail flag was specified!")
    } else {
      if (failed) {
        sys.exit(1)
      }
    }
  }

  private def defaultTests(
      commonConfig: PlatformApplications.Config,
      toolConfig: Config,
      backends: Set[LedgerBackend]): Map[String, () => Suite] = {
    val semanticTestsRunner = lazyInit(
      "SemanticTests",
      name =>
        new SandboxSemanticTestsLfRunner {
          override def suiteName: String = name
          override def actorSystemName = s"${name}TestToolActorSystem"
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override implicit lazy val patienceConfig: PatienceConfig =
            PatienceConfig(Span(60L, Seconds))
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(semanticTestsResource))
      }
    )

    Map(
      semanticTestsRunner
    )
  }
  private def optionalTests(
      commonConfig: PlatformApplications.Config,
      toolConfig: Config,
      backends: Set[LedgerBackend]): Map[String, () => Suite] = {

    val transactionServiceIT = lazyInit(
      "TransactionServiceTests",
      name =>
        new TransactionServiceIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val transactionBackpressureIT = lazyInit(
      "TransactionBackpressureIT",
      name =>
        new TransactionBackpressureIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val divulgenceIT = lazyInit(
      "DivulgenceIT",
      name =>
        new DivulgenceIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val commandTransactionChecksHighLevelIT = lazyInit(
      "CommandTransactionChecksHighLevelIT",
      name =>
        new CommandTransactionChecksHighLevelIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val commandTransactionChecksLowLevelIT = lazyInit(
      "CommandTransactionChecksLowLevelIT",
      name =>
        new CommandTransactionChecksLowLevelIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override def fixtureIdsEnabled: Set[LedgerBackend] = backends
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val packageManagementServiceIT = lazyInit(
      "PackageManagementServiceIT",
      name =>
        new PackageManagementServiceIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def fixtureIdsEnabled: Set[LedgerBackend] = Set(LedgerBackend.RemoteApiProxy)
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    val partyManagementServiceIT = lazyInit(
      "PartyManagementServiceIT",
      name =>
        new PartyManagementServiceIT {
          override def suiteName: String = name
          override def actorSystemName = s"${name}ToolActorSystem"
          override def fixtureIdsEnabled: Set[LedgerBackend] = Set(LedgerBackend.RemoteApiProxy)
          override def spanScaleFactor: Double = toolConfig.timeoutScaleFactor
          override protected def config: Config =
            commonConfig.withDarFile(resourceAsFile(integrationTestResource))
      }
    )

    Map(
      transactionServiceIT,
      transactionBackpressureIT,
      divulgenceIT,
      commandTransactionChecksHighLevelIT,
      commandTransactionChecksLowLevelIT,
      packageManagementServiceIT,
      partyManagementServiceIT
    )
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

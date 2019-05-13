// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.daml.lf.UniversalArchiveReader
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.platform.apitesting.{
  LedgerBackend,
  LedgerContext,
  PlatformChannels,
  RemoteServerResource
}
import com.digitalasset.platform.semantictest.SemanticTestAdapter
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.platform.tests.integration.ledger.api.TransactionServiceIT

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.breakOut
import scala.util.Random

object LedgerApiTestToolHelper {
  def runWithTimeout[T](timeout: Duration)(f: => T)(implicit ec: ExecutionContext): Option[T] = {
    Await.result(Future(f), timeout).asInstanceOf[Option[T]]
  }

  def runWithTimeout[T](timeout: Duration, default: T)(f: => T)(
      implicit ec: ExecutionContext): T = {
    runWithTimeout(timeout)(f).getOrElse(default)
  }
}

object LedgerApiTestTool {

  def main(args: Array[String]): Unit = {
    implicit val toolSystem: ActorSystem = ActorSystem("LedgerApiTestTool")
    implicit val toolMaterializer: ActorMaterializer = ActorMaterializer()(toolSystem)
    implicit val ec: ExecutionContext = toolMaterializer.executionContext
    implicit val esf: AkkaExecutionSequencerPool =
      new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(toolSystem)

    val testResources = List("/ledger/ledger-api-integration-tests/SemanticTests.dar")

    val toolConfig = Cli
      .parse(args)
      .getOrElse(sys.exit(1))

    if (toolConfig.extract) {
      extractTestFiles(testResources)
      System.exit(0)
    }

    var failed = false

    val packages: Map[PackageId, Ast.Package] = testResources
      .flatMap(loadAllPackagesFromResource)(breakOut)

    //    val scenarios = SemanticTester.scenarios(packages)
    val scenarios: Map[PackageId, Iterable[QualifiedName]] = Map.empty
    val nScenarios: Int = scenarios.foldLeft(0)((c, xs) => c + xs._2.size)

    println(s"Running $nScenarios scenarios against ${toolConfig.host}:${toolConfig.port}...")

    val ledgerResource =
      RemoteServerResource(toolConfig.host, toolConfig.port, toolConfig.tlsConfig)
        .map {
          case PlatformChannels(channel) =>
            LedgerContext.SingleChannelContext(channel, None, packages.keys)
        }
    ledgerResource.setup()
    val ledger = ledgerResource.value

    if (toolConfig.performReset) {
      Await.result(ledger.reset(), 10.seconds)
    }

    try {
      val runSuffix = Random.alphanumeric.take(10).mkString
      val partyNameMangler = (partyText: String) => s"$partyText-$runSuffix"
      val commandIdMangler: ((QualifiedName, Int, L.NodeId) => String) =
        (scenario, stepId, nodeId) => s"ledger-api-test-tool-$scenario-$stepId-$nodeId-$runSuffix"

      scenarios.foreach {
        case (pkgId, names) =>
          val tester = new SemanticTester(
            parties =>
              new SemanticTestAdapter(
                ledger,
                packages,
                parties,
                timeoutScaleFactor = toolConfig.timeoutScaleFactor),
            pkgId,
            packages,
            partyNameMangler,
            commandIdMangler
          )
          names
            .foreach { name =>
              println(s"Testing scenario: $name")
              val _ = try {
                Await.result(
                  tester.testScenario(name),
                  (60 * toolConfig.timeoutScaleFactor).seconds
                )
              } catch {
                case (t: Throwable) =>
                  val sw = new StringWriter
                  t.printStackTrace(new PrintWriter(sw))
                  sys.error(
                    s"Running scenario $name failed with: " + t
                      .getMessage() + "\n\nWith stacktrace:\n" + sw
                      .toString() + "\n\nTesting tool own stacktrace is:")
              }
            }
      }
      println("All scenarios completed.")

      val integrationTestResource = "/ledger/sandbox/Test.dar"
      val is = getClass.getResourceAsStream(integrationTestResource)
      if (is == null) sys.error(s"Could not find $integrationTestResource in classpath")
      val targetPath: Path = Files.createTempFile("ledger-api-test-tool-", "-test.dar")
      Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);

      val tsit = new TransactionServiceIT {
        override def fixtureIdsEnabled: Set[LedgerBackend] =
          Set(LedgerBackend.RemoteAPIProxy)

        override protected def getSystem: ActorSystem = toolSystem
        override protected def getMaterializer: ActorMaterializer = toolMaterializer

        override protected def config: Config =
          Config
            .defaultWithTimeProvider(TimeProviderType.WallClock)
            .withDynamicLedgerId()
            .withHost(toolConfig.host)
            .withPort(toolConfig.port)
            .withTlsConfigOption(toolConfig.tlsConfig)
            .withDarFile(targetPath)
      }
//      org.scalatest.run()
//      println(tsit.testNames)
      LedgerApiTestToolHelper.runWithTimeout(1.seconds) {
        Some(tsit.execute(
          "Transaction Service when querying ledger end should return the value if ledger Ids match"))
      }

    } catch {
      case (t: Throwable) =>
        failed = true
        if (!toolConfig.mustFail) throw t
    } finally {
      ledgerResource.close()
      toolMaterializer.shutdown()
      val _ = Await.result(toolSystem.terminate(), 5.seconds)
    }

    if (toolConfig.mustFail) {
      if (failed) println("One or more scenarios failed as expected.")
      else
        throw new RuntimeException(
          "None of the scenarios failed, yet the --must-fail flag was specified!")
    }
  }

  private def loadAllPackagesFromResource(resource: String): Map[PackageId, Ast.Package] = {
    // TODO: replace with stream-supporting functions from UniversalArchiveReader when
    // https://github.com/digital-asset/daml/issues/547 is fixed
    val is = getClass.getResourceAsStream(resource)
    if (is == null) sys.error(s"Could not find $resource in classpath")
    val targetPath: Path = Files.createTempFile("ledger-api-test-tool-", "-test.dar")
    Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
    val f: File = targetPath.toFile
    if (f == null) sys.error(s"Could not open $targetPath")
    val packages = UniversalArchiveReader().readFile(f).get
    Map(packages.all.map {
      case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
    }: _*)
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

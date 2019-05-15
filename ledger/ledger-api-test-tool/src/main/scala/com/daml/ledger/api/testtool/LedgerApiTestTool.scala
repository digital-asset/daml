// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.daml.lf.UniversalArchiveReader
import com.digitalasset.daml.lf.types.{LedgerForScenarios => L}
import com.digitalasset.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.platform.apitesting.{LedgerContext, PlatformChannels, RemoteServerResource}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.semantictest.SemanticTestAdapter

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.collection.breakOut
import scala.util.Random

object LedgerApiTestTool {

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("LedgerApiTestTool")
    implicit val mat: ActorMaterializer = ActorMaterializer()(system)
    implicit val ec: ExecutionContext = mat.executionContext
    implicit val esf: AkkaExecutionSequencerPool =
      new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

    val testResources = List("/ledger/ledger-api-integration-tests/SemanticTests.dar")

    val config = Cli
      .parse(args)
      .getOrElse(sys.exit(1))

    if (config.extract) {
      extractTestFiles(testResources)
      System.exit(0)
    }

    val packages: Map[PackageId, Ast.Package] = testResources
      .flatMap(loadAllPackagesFromResource)(breakOut)

    val scenarios = SemanticTester.scenarios(packages)
    val nScenarios: Int = scenarios.foldLeft(0)((c, xs) => c + xs._2.size)

    println(s"Running $nScenarios scenarios against ${config.host}:${config.port}...")

    val ledgerResource = RemoteServerResource(config.host, config.port, config.tlsConfig)
      .map {
        case PlatformChannels(channel) =>
          LedgerContext.SingleChannelContext(channel, LedgerIdMode.Dynamic(), packages.keys)
      }
    ledgerResource.setup()
    val ledger = ledgerResource.value

    if (config.performReset) {
      Await.result(ledger.reset(), 10.seconds)
    }
    var failed = false

    val runSuffix = "-" + Random.alphanumeric.take(10).mkString
    val partyNameMangler = (partyText: String) => partyText + runSuffix
    val commandIdMangler: ((QualifiedName, Int, L.ScenarioNodeId) => String) =
      (scenario, stepId, nodeId) => s"ledger-api-test-tool-$scenario-$stepId-$nodeId-$runSuffix"

    try {
      scenarios.foreach {
        case (pkgId, names) =>
          val tester = new SemanticTester(
            parties =>
              new SemanticTestAdapter(
                ledger,
                packages,
                parties,
                timeoutScaleFactor = config.timeoutScaleFactor),
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
                  (60 * config.timeoutScaleFactor).seconds
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
    } catch {
      case (t: Throwable) =>
        failed = true
        if (!config.mustFail) throw t
    } finally {
      ledgerResource.close()
      mat.shutdown()
      val _ = Await.result(system.terminate(), 5.seconds)
    }

    if (config.mustFail) {
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

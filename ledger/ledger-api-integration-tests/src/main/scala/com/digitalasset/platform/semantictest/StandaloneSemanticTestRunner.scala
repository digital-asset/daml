// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import java.io.{BufferedInputStream, File, FileInputStream}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.platform.apitesting.{LedgerContext, PlatformChannels, RemoteServerResource}
import com.digitalasset.platform.sandbox.config.{DamlPackageContainer, SandboxConfig}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object StandaloneSemanticTestRunner {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("SemanticTestRunner")
    implicit val mat: ActorMaterializer = ActorMaterializer()(system)
    implicit val ec: ExecutionContext = mat.executionContext
    implicit val esf: AkkaExecutionSequencerPool =
      new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

    val config = argParser
      .parse(args, defaultConfig)
      .getOrElse(sys.exit(1))

    val packages: Map[PackageId, Ast.Package] = config.packageContainer.packages
    val scenarios = SemanticTester.scenarios(packages)
    val nScenarios: Int = scenarios.foldLeft(0)((c, xs) => c + xs._2.size)

    println(s"Running ${nScenarios} scenarios against ${config.host}:${config.port}...")

    val ledgerResource = RemoteServerResource(config.host, config.port)
      .map {
        case PlatformChannels(channel) =>
          LedgerContext.SingleChannelContext(channel, None, packages.keys)
      }
    ledgerResource.setup()
    val ledger = ledgerResource.value

    if (config.performReset) {
      Await.result(ledger.reset(), 10.seconds)
    }

    scenarios.foreach {
      case (pkgId, names) =>
        val tester = new SemanticTester(
          parties => new SemanticTestAdapter(ledger, packages, parties.map(_.underlyingString)),
          pkgId,
          packages)
        names.foreach { name =>
          println(s"Testing scenario: $name")
          val _ = Await.result(
            tester.testScenario(name),
            10.seconds
          )
        }
    }
    println("All scenarios completed.")
    ledgerResource.close()
    mat.shutdown()
    val _ = Await.result(system.terminate(), 5.seconds)
  }

  private def readPackage(f: File): (PackageId, Ast.Package) = {
    val is = new BufferedInputStream(new FileInputStream(f))
    try {
      Decode.decodeArchiveFromInputStream(is)
    } finally {
      is.close()
    }
  }

  final case class Config(
      host: String,
      port: Int,
      packageContainer: DamlPackageContainer,
      performReset: Boolean)

  private val defaultConfig = Config(
    host = "localhost",
    port = SandboxConfig.DefaultPort,
    packageContainer = DamlPackageContainer(),
    performReset = false,
  )

  private val argParser = new scopt.OptionParser[Config]("semantic-test-runner") {
    head("Semantic test runner")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text(s"Ledger API server port. Defaults to ${SandboxConfig.DefaultPort}.")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("Ledger API server host. Defaults to localhost.")

    opt[Unit]('r', "reset")
      .action((_, c) => c.copy(performReset = true))
      .text("Perform a ledger reset before running the tests. Defaults to false.")

    arg[File]("<archive>...")
      .unbounded()
      .action((f, c) => c.copy(packageContainer = c.packageContainer.withFile(f)))
      .text("DAML-LF Archives to load and run all scenarios from.")
  }

}

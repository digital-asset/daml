// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.apitesting._
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

    val ledgerResource: Resource[LedgerContext] = config.mapping match {
      case Some(file) => {
        println(s"Running ${nScenarios} scenarios against server mapping file ${file.getAbsolutePath}...")
        MultiRemoteServerResource.fromConfig(file, Ref.Party.assertFromString(config.defaultParty), packages, esf)
      }
      case None => {
        println(s"Running ${nScenarios} scenarios against ${config.host}:${config.port}...")
        SingleRemoteServerResource.fromHostAndPort(config.host, config.port, packages, esf)
      }
    }

    ledgerResource.setup()
    val ledger: LedgerContext = ledgerResource.value

    if (config.performReset) {
      Await.result(ledger.reset(), 10.seconds)
    }

    scenarios.foreach {
      case (pkgId, names) =>
        val tester = new SemanticTester(
          parties => new SemanticTestAdapter(ledger, packages, parties),
          pkgId,
          packages)
        names.foreach { name =>
          println(s"Testing scenario: $name")
          val _ = Await.result(
            tester.testScenario(name),
            1200.seconds
          )
        }
    }
    println("All scenarios completed.")
    ledgerResource.close()
    mat.shutdown()
    val _ = Await.result(system.terminate(), 5.seconds)
  }

  final case class Config(
      mapping: Option[File],
      defaultParty: String,
      host: String,
      port: Int,
      packageContainer: DamlPackageContainer,
      performReset: Boolean)

  private val defaultConfig = Config(
    mapping = None,
    defaultParty = "OPERATOR",
    host = "localhost",
    port = SandboxConfig.DefaultPort,
    packageContainer = DamlPackageContainer(),
    performReset = false,
  )

  private val argParser = new scopt.OptionParser[Config]("semantic-test-runner") {
    head("Semantic test runner")

    opt[File]('m', "mapping")
      .action((x, c) => c.copy(mapping = Some(x)))
      .text(s"Ledger API server mapping. Defaults to a single host and port for all parties.")

    opt[String]('p', "default-party")
      .action((x, c) => c.copy(defaultParty = x))
      .text("Default party used for e.g. ledgerId and getTime calls. Defaults to OPERATOR.")

    opt[String]("host")
      .action((x, c) => c.copy(host = x))
      .text("Ledger API server host used if no mapping provided. Defaults to localhost.")

    opt[Int]("port")
      .action((x, c) => c.copy(port = x))
      .text(s"Ledger API server port used if no mapping provided. Defaults to ${SandboxConfig.DefaultPort}.")

    opt[Unit]('r', "reset")
      .action((_, c) => c.copy(performReset = true))
      .text("Perform a ledger reset before running the tests. Defaults to false.")

    arg[File]("<archive>...")
      .unbounded()
      .action((f, c) => c.copy(packageContainer = c.packageContainer.withFile(f)))
      .text("DAML-LF Archives to load and run all scenarios from.")
  }

}

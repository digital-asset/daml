// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.model.test.LedgerRunner.ApiPorts
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val languageVersion = LanguageVersion.v2_dev

  private val universalDarPaths: List[String] = for {
    pkgVersion <- List(1, 2, 3)
  } yield {
    val mangledLanguageVersion = languageVersion.pretty.replaceAll("\\.", "")
    rlocation(
      s"daml-lf/model-test-lib/universal-pkgv${pkgVersion}-lfv${mangledLanguageVersion}.dar"
    )
  }

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val cantonLedgerRunner = LedgerRunner.forCantonLedger(
      languageVersion,
      universalDarPaths,
      "localhost",
      List(
        ApiPorts(5011, 5012),
        ApiPorts(5013, 5014),
        ApiPorts(5015, 5016),
      ),
    )

    val bad = Parser.parseScenario("""
        |Scenario
        |  Topology
        |    Participant 0 parties={1}
        |  Ledger
        |    Commands participant=0 actAs={1} disclosures={}
        |      Create 0 sigs={1} obs={}
        |    Commands participant=0 actAs={1} disclosures={}
        |      Exercise NonConsuming 0 ctl={1} cobs={}
        |      Exercise NonConsuming 0 pkg=1 ctl={1} cobs={}
        |""".stripMargin)

    println(Pretty.prettyScenario(bad))
    cantonLedgerRunner.runAndProject(bad) match {
      case Left(error) => println(error.pretty)
      case Right(_) => println("success")
    }

    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}

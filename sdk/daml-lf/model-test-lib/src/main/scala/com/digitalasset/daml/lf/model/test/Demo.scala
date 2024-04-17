// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.lf.model.test.LedgerRunner.ApiPorts
import com.daml.lf.model.test.Ledgers.Scenario
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val universalDarPath: String = rlocation("daml-lf/model-test-lib/universal.dar")

  def main(args: Array[String]): Unit = {

    val ledgers = Enumerations.ledgers(50)
    val card = ledgers.cardinal

    def randomBigIntLessThan(n: BigInt): BigInt = {
      var res: BigInt = BigInt(0)
      do {
        res = BigInt(n.bitLength, new scala.util.Random())
      } while (res >= n)
      res
    }

    def randomScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      Gen
        .resize(5, new Generators(3, 5).scenarioGen)
        .sample
    }.flatten
    val _ = randomScenarios

    def validLedgersSym: LazyList[Ledgers.Ledger] = LazyList.continually {
      val skeleton = ledgers(randomBigIntLessThan(card))
      SymbolicSolver.solve(skeleton, 4)
    }.flatten
    val _ = validLedgersSym

    def validScenarios: LazyList[Scenario] = LazyList.continually {
      val randomSkeleton = ledgers(randomBigIntLessThan(card))
      new LedgerFixer(3, 10).fixLedger(randomSkeleton).sample
    }.flatten
    val _ = validScenarios

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val cantonLedgerRunner = LedgerRunner.forCantonLedger(
      universalDarPath,
      "localhost",
      List(ApiPorts(5011, 5012), ApiPorts(5013, 5014), ApiPorts(5015, 5016)),
    )
    val ideLedgerRunner = LedgerRunner.forIdeLedger(universalDarPath)

    while (true) {
      validScenarios
        .foreach(scenario => {
          if (scenario.ledger.nonEmpty) {
            println("\n==== ledger ====")
            println(Pretty.prettyScenario(scenario))
            ideLedgerRunner.runAndProject(scenario) match {
              case Left(error) =>
                println("INVALID LEDGER!")
                println(Pretty.prettyScenario(scenario))
                println(error.pretty)
                println(scenario)
                System.exit(1)
              case Right(ideProjections) =>
                println("==== ide ledger ====")
                ideProjections.foreach { case (partyId, projection) =>
                  println(s"Projection for party $partyId")
                  println(Pretty.prettyProjection(projection))
                }
                println("==== canton ====")
                cantonLedgerRunner.runAndProject(scenario) match {
                  case Left(error) =>
                    println("ERROR")
                    println(error.pretty)
                    println(scenario)
                    System.exit(1)
                  case Right(cantonProjections) =>
                    if (cantonProjections == ideProjections) {
                      println("MATCH!")
                    } else {
                      println("MISMATCH!")
                      cantonProjections.foreach { case (partyId, projection) =>
                        println(s"Projection for party $partyId")
                        println(Pretty.prettyProjection(projection))
                      }
                      println(scenario)
                      System.exit(1)
                    }
                }
            }
          }
        })
    }
    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}

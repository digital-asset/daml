// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.model.test.LedgerRunner.ApiPorts
import com.daml.lf.model.test.Ledgers.Scenario
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val languageVersion = LanguageVersion.v2_dev

  private val universalDarPath: String = rlocation(
    s"daml-lf/model-test-lib/universal-v${languageVersion.pretty.replaceAll("\\.", "")}.dar"
  )

  def main(args: Array[String]): Unit = {

    val scenarios =
      new Enumerations(languageVersion).scenarios(numParticipants = 3, numCommands = 4)(50)

    def randomBigIntLessThan(n: BigInt): BigInt = {
      var res: BigInt = BigInt(0)
      do {
        res = BigInt(n.bitLength, new scala.util.Random())
      } while (res >= n)
      res
    }

    def randomScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      Gen
        .resize(5, new Generators(numParticipants = 3, numParties = 6).scenarioGen)
        .sample
    }.flatten
    val _ = randomScenarios

    def validSymScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      val skeleton = scenarios(randomBigIntLessThan(scenarios.cardinal))
      SymbolicSolver.solve(skeleton, numParties = 5)
    }.flatten
    val _ = validSymScenarios

    def validScenarios: LazyList[Scenario] = LazyList.continually {
      val randomSkeleton = scenarios(randomBigIntLessThan(scenarios.cardinal))
      new LedgerFixer(numParties = 5).fixScenario(randomSkeleton).sample
    }.flatten
    val _ = validScenarios

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val cantonLedgerRunner = LedgerRunner.forCantonLedger(
      languageVersion,
      universalDarPath,
      "localhost",
      List(
        ApiPorts(5011, 5012),
        ApiPorts(5013, 5014),
        ApiPorts(5015, 5016),
      ),
    )
    val ideLedgerRunner = LedgerRunner.forIdeLedger(languageVersion, universalDarPath)

    // val workers = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

    validSymScenarios
      .foreach(scenario => {
        // workers.execute(() =>
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
              println("VALID!")
              // ideProjections.foreach { case (partyId, projection) =>
              // println(s"Projection for party $partyId")
              // println(Pretty.prettyProjection(projection))
              // }
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
                    cantonProjections.foreach { case (partyId, projections) =>
                      projections.foreach { case (participantId, projection) =>
                        println(s"Projection for party $partyId on participant $participantId")
                        println(Pretty.prettyProjection(projection))
                      }
                    }
                    println(scenario)
                    System.exit(1)
                  }
              }
          }
        }
        // )
      })
    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}

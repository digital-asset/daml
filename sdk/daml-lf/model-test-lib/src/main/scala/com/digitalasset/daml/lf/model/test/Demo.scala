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
import org.scalacheck.{Gen, Prop}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val languageVersion = LanguageVersion.v2_dev
  private val alsoRunOnCanton = true
  private val numParticipants = 2
  private val numParties = 2
  private val numPackages = 3

  private val universalDarPaths: List[String] = for {
    pkgVersion <- List(1, 2, 3)
  } yield {
    val mangledLanguageVersion = languageVersion.pretty.replaceAll("\\.", "")
    rlocation(
      s"daml-lf/model-test-lib/universal-pkgv${pkgVersion}-lfv${mangledLanguageVersion}.dar"
    )
  }

  def main(args: Array[String]): Unit = {

    val scenarios =
      new Enumerations(languageVersion).scenarios(numParticipants)(20)

    def randomBigIntLessThan(n: BigInt): BigInt = {
      var res: BigInt = BigInt(0)
      do {
        res = BigInt(n.bitLength, new scala.util.Random())
      } while (res >= n)
      res
    }

    def randomScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      Gen
        .resize(5, new Generators(numParticipants, numPackages, numParties).scenarioGen)
        .sample
    }.flatten
    val _ = randomScenarios

    def validSymScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      val skeleton = scenarios(randomBigIntLessThan(scenarios.cardinal))
      SymbolicSolver.solve(skeleton, numPackages, numParties)
    }.flatten
    val _ = validSymScenarios

    def validScenarios: LazyList[Ledgers.Scenario] = LazyList.continually {
      val randomSkeleton = scenarios(randomBigIntLessThan(scenarios.cardinal))
      new LedgerFixer(numPackages, numParties).fixScenario(randomSkeleton).sample
    }.flatten
    val _ = validScenarios

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
    val ideLedgerRunner = LedgerRunner.forIdeLedger(languageVersion, universalDarPaths)

    // val workers = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

    // @nowarn("cat=unused")
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

    validSymScenarios
    List(bad)
      .foreach(scenario => {
        // workers.execute(() =>
        if (scenario.ledger.nonEmpty) {
          println("\n==== ledger ====")
          println(Pretty.prettyScenario(scenario))
          assert(SymbolicSolver.valid(scenario, numPackages, numParties))
          cantonLedgerRunner.runAndProject(scenario) match {
            case Left(error) =>
              println("INVALID LEDGER!")
              println(Pretty.prettyScenario(scenario))
              println(error.pretty)
              println(scenario)
              println("shrinking")
              Prop
                .forAllShrink(Gen.const(scenario), Shrinkers.shrinkScenario.shrink)(s =>
                  ideLedgerRunner.runAndProject(s).isRight
                )
                .check()
              System.exit(1)
            case Right(ideProjections) =>
              println("==== ide ledger ====")
              println("VALID!")
              //              ideProjections.foreach { case (partyId, projections) =>
              //                projections.foreach { case (participantId, projection) =>
              //                  println(s"Projection for party $partyId, participant $participantId")
              //                  println(Pretty.prettyProjection(projection))
              //                }
              //              }
              if (alsoRunOnCanton) {
                println("==== canton ====")
                cantonLedgerRunner.runAndProject(scenario) match {
                  case Left(error) =>
                    println("ERROR")
                    println(error.pretty)
                    println(scenario)
                    println("shrinking")
                    Prop
                      .forAllShrink(Gen.const(scenario), Shrinkers.shrinkScenario.shrink)(s => {
                        val res = cantonLedgerRunner.runAndProject(s)
                        res.isRight || {
                          println(res)
                          false
                        }
                      })
                      .check()
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
                      println("shrinking")
                      Prop
                        .forAllShrink(Gen.const(scenario), Shrinkers.shrinkScenario.shrink)(s =>
                          ideLedgerRunner.runAndProject(s) != cantonLedgerRunner.runAndProject(s)
                        )
                        .check()
                      System.exit(1)
                    }
                }
              }
          }
        }
        // )
      })
    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.lf.model.test.Ledgers.Ledger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val universalDarPath: String = rlocation("daml-lf/model-test-lib/universal.dar")

  def main(args: Array[String]): Unit = {

    val ledgers = Enumerations.ledgers(100)
    val card = ledgers.cardinal

    def validLedgersSym: LazyList[Ledgers.Ledger] = LazyList.continually {
      val randomIndex = {
        var res: BigInt = BigInt(0)
        do {
          res = BigInt(card.bitLength, new scala.util.Random())
        } while (res >= card)
        res
      }
      val skeleton = ledgers(randomIndex)
      SymbolicSolver.solve(skeleton, 4)
    }.flatten

    def validLedgers: LazyList[Ledger] = LazyList.continually {
      val randomIndex = {
        var res: BigInt = BigInt(0)
        do {
          res = BigInt(card.bitLength, new scala.util.Random())
        } while (res > card)
        res
      }
      val randomLedger = ledgers(randomIndex)
      new LedgerFixer(5).fixLedger(randomLedger).sample
    }.flatten
    print(validLedgers.isEmpty)

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val cantonLedgerRunner = LedgerRunner.forCantonLedger(universalDarPath, "localhost", 5011, 5012)
    val ideLedgerRunner = LedgerRunner.forIdeLedger(universalDarPath)

    while (true) {
      Gen
        .resize(5, new Generators(3).ledgerGen)
        .sample
      validLedgersSym
        .foreach(ledger => {
          if (ledger.nonEmpty) {
            ideLedgerRunner.runAndProject(ledger) match {
              case Left(error) =>
                println("INVALID LEDGER!")
                println(Pretty.prettyLedger(ledger))
                println(error.pretty)
                println(ledger)
                System.exit(1)
              case Right(ideProjections) =>
                println("\n==== ledger ====")
                println(Pretty.prettyLedger(ledger))
                println("==== ide ledger ====")
                ideProjections.foreach { case (partyId, projection) =>
                  println(s"Projection for party $partyId")
                  println(Pretty.prettyProjection(projection))
                }
                println("==== canton ====")
                cantonLedgerRunner.runAndProject(ledger) match {
                  case Left(error) =>
                    println("ERROR")
                    println(error.pretty)
                    println(ledger)
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
                      println(ledger)
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

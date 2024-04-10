// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val universalDarPath: String = rlocation("daml-lf/model-test-lib/universal.dar")

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val cantonLedgerRunner = LedgerRunner.forCantonLedger(universalDarPath, "localhost", 5011, 5012)
    val ideLedgerRunner = LedgerRunner.forIdeLedger(universalDarPath)

    val test: Ledgers.Ledger =
      List(
        Ledgers.Commands(
          actAs = Set(1, 2),
          actions = List(
            Ledgers.Create(contractId = 1, signatories = Set(1), observers = Set()),
            Ledgers.Create(contractId = 2, signatories = Set(2), observers = Set()),
          ),
        ),
        Ledgers.Commands(
          actAs = Set(2),
          actions = List(
            Ledgers.Exercise(
              contractId = 2,
              kind = Ledgers.Consuming,
              controllers = Set(2),
              choiceObservers = Set(),
              subTransaction = List(
                Ledgers.Exercise(
                  contractId = 1,
                  kind = Ledgers.Consuming,
                  controllers = Set(2),
                  choiceObservers = Set(),
                  subTransaction = List(),
                )
              ),
            )
          ),
        ),
      )

    println(test)
    while (true) {
      Gen
        .resize(5, new Generators(3).ledgerGen)
        .sample
        .foreach(ledger => {
          if (ledger.nonEmpty) {
            ideLedgerRunner.runAndProject(ledger) match {
              case Left(_) =>
                print(".")
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
                  case Right(cantonProjections) =>
                    if (cantonProjections == ideProjections) {
                      println("MATCH!")
                    } else {
                      println("MISMATCH!")
                      cantonProjections.foreach { case (partyId, projection) =>
                        println(s"Projection for party $partyId")
                        println(Pretty.prettyProjection(projection))
                      }
                    }
                }
            }
          }
        })
    }
    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}

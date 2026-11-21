// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.submission.foo.RandomPartySelecting

import scala.concurrent.{ExecutionContext, Future}

class FooSubmission(
    submitter: CommandSubmitter,
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    allocatedParties: AllocatedParties,
    names: Names,
    randomnessProvider: RandomnessProvider,
) {

  def performSubmission(submissionConfig: FooSubmissionConfig)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val (divulgerCmds, divulgeesToDivulgerKeyMap) = FooDivulgerCommandGenerator
      .makeCreateDivulgerCommands(
        divulgingParty = allocatedParties.signatory,
        allDivulgees = allocatedParties.divulgees,
      )
    val partySelecting =
      new RandomPartySelecting(
        config = submissionConfig,
        allocatedParties = allocatedParties,
        randomnessProvider = randomnessProvider,
      )
    for {
      _ <-
        if (divulgerCmds.nonEmpty) {
          require(
            divulgeesToDivulgerKeyMap.nonEmpty,
            "Map from divulgees to Divulger contract keys must be non empty.",
          )
          submitter.submitSingleBatch(
            commandId = "divulgence-setup",
            actAs = Seq(allocatedParties.signatory) ++ allocatedParties.divulgees,
            commands = divulgerCmds,
          )
        } else {
          Future.unit
        }
      generator: CommandGenerator = new FooCommandGenerator(
        config = submissionConfig,
        divulgeesToDivulgerKeyMap = divulgeesToDivulgerKeyMap,
        names = names,
        allocatedParties = allocatedParties,
        partySelecting = partySelecting,
        randomnessProvider = randomnessProvider,
      )
      _ <- submitter
        .generateAndSubmit(
          generator = generator,
          config = submissionConfig,
          baseActAs = List(allocatedParties.signatory) ++ allocatedParties.divulgees,
          maxInFlightCommands = maxInFlightCommands,
          submissionBatchSize = submissionBatchSize,
        )
    } yield ()
  }
}

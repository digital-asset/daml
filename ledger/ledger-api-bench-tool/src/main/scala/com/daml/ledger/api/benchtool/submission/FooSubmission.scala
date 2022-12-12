// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.api.benchtool.submission.foo.RandomPartySelecting

import scala.concurrent.{ExecutionContext, Future}

class FooSubmission(
    submitter: CommandSubmitter,
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    submissionConfig: FooSubmissionConfig,
    allocatedParties: AllocatedParties,
    names: Names,
    partySelectingRandomnessProvider: RandomnessProvider = RandomnessProvider.Default,
    consumingEventsRandomnessProvider: RandomnessProvider = RandomnessProvider.Default,
) {

  def performSubmission()(implicit
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
        randomnessProvider = partySelectingRandomnessProvider,
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
        defaultRandomnessProvider = RandomnessProvider.Default,
        config = submissionConfig,
        divulgeesToDivulgerKeyMap = divulgeesToDivulgerKeyMap,
        names = names,
        allocatedParties = allocatedParties,
        partySelecting = partySelecting,
        consumingEventsRandomnessProvider = consumingEventsRandomnessProvider,
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

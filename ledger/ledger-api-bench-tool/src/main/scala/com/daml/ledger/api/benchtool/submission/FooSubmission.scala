// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.client.binding

import scala.concurrent.{ExecutionContext, Future}

class FooSubmission(
    submitter: CommandSubmitter,
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    submissionConfig: FooSubmissionConfig,
    signatory: binding.Primitive.Party,
    allObservers: List[binding.Primitive.Party],
    allDivulgees: List[binding.Primitive.Party],
) {

  def performSubmission()(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val divulgenceGenerator = new FooDivulgerCommandGenerator()
    val (divulgerCmds, divulgeesToDivulgerKeyMap) = divulgenceGenerator
      .makeCreateDivulgerCommands(
        divulgingParty = signatory,
        allDivulgees = allDivulgees,
      )

    for {
      _ <-
        if (divulgeesToDivulgerKeyMap.nonEmpty) {
          submitter.submitSingleBatch(
            commandId = "divulgence-setup",
            actAs = Seq(signatory) ++ allDivulgees,
            commands = divulgerCmds,
          )
        } else {
          Future.unit
        }
      generator: CommandGenerator = new FooCommandGenerator(
        randomnessProvider = RandomnessProvider.Default,
        signatory = signatory,
        config = submissionConfig,
        allObservers = allObservers,
        allDivulgees = allDivulgees,
        divulgeesToDivulgerKeyMap = divulgeesToDivulgerKeyMap,
      )
      _ <- submitter
        .generateAndSubmit(
          generator = generator,
          config = submissionConfig,
          actAs = List(signatory) ++ allDivulgees,
          maxInFlightCommands = maxInFlightCommands,
          submissionBatchSize = submissionBatchSize,
        )
    } yield ()
  }
}

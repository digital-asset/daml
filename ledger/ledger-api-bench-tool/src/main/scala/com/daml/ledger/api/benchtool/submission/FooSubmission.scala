// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.client.binding

import scala.concurrent.{ExecutionContext, Future}

/** Generates and submits Foo and related commands
  */
class FooSubmission(
    submitter: CommandSubmitter,
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    submissionConfig: FooSubmissionConfig,
    signatory: binding.Primitive.Party,
    allObservers: List[binding.Primitive.Party],
) {

  def performSubmission()(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val generator = new FooCommandGenerator(
      randomnessProvider = RandomnessProvider.Default,
      signatory = signatory,
      config = submissionConfig,
      allObservers = allObservers,
    )
    for {
      _ <- submitter
        .generateAndSubmit(
          generator = generator,
          config = submissionConfig,
          signatory = signatory,
          maxInFlightCommands = maxInFlightCommands,
          submissionBatchSize = submissionBatchSize,
        )
    } yield ()
  }

}

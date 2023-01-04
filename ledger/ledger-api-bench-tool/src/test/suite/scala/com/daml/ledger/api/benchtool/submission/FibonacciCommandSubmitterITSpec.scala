// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.BenchtoolSandboxFixture
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class FibonacciCommandSubmitterITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues {

  it should "populate create fibonacci contracts" in {

    val config = WorkflowConfig.FibonacciSubmissionConfig(
      numberOfInstances = 10,
      uniqueParties = false,
      value = 7,
      waitForSubmission = true,
    )

    for {
      (apiServices, names, submitter) <- benchtoolFixture()
      allocatedParties <- submitter.prepare(config)
      _ = allocatedParties.divulgees shouldBe empty
      generator = new FibonacciCommandGenerator(
        signatory = allocatedParties.signatory,
        config = config,
        names = names,
      )
      _ <- submitter.generateAndSubmit(
        generator = generator,
        config = config,
        baseActAs = List(allocatedParties.signatory) ++ allocatedParties.divulgees,
        maxInFlightCommands = 1,
        submissionBatchSize = 5,
      )
      eventsObserver = TreeEventsObserver(expectedTemplateNames =
        Set(
          "InefficientFibonacci",
          "InefficientFibonacciResult",
        )
      )
      _ <- apiServices.transactionService.transactionTrees(
        config = WorkflowConfig.StreamConfig.TransactionTreesStreamConfig(
          name = "dummy-name",
          filters = List(
            WorkflowConfig.StreamConfig.PartyFilter(
              party = allocatedParties.signatory.toString,
              templates = List.empty,
              interfaces = List.empty,
            )
          ),
          beginOffset = None,
          endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
          objectives = None,
          maxItemCount = None,
          timeoutInSecondsO = None,
        ),
        observer = eventsObserver,
      )
      observerResult <- eventsObserver.result
    } yield {
      observerResult.numberOfCreatesPerTemplateName(
        "InefficientFibonacci"
      ) shouldBe config.numberOfInstances withClue ("number of create events")
      succeed
    }
  }

}

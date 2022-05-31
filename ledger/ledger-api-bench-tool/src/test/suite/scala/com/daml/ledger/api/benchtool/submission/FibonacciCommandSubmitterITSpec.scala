// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class FibonacciCommandSubmitterITSpec
    extends AsyncFlatSpec
    with SandboxFixture
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
      ledgerApiServicesF <- LedgerApiServices.forChannel(
        channel = channel,
        authorizationHelper = None,
      )
      apiServices = ledgerApiServicesF("someUser")
      names = new Names()
      tested = CommandSubmitter(
        names = names,
        benchtoolUserServices = apiServices,
        adminServices = apiServices,
        metricRegistry = new MetricRegistry,
        metricsManager = NoOpMetricsManager(),
        waitForSubmission = config.waitForSubmission,
      )
      allocatedParties <- tested.prepare(config)
      _ = allocatedParties.divulgees shouldBe empty
      generator = new FibonacciCommandGenerator(
        signatory = allocatedParties.signatory,
        config = config,
        names = names,
      )
      _ <- tested.generateAndSubmit(
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
            )
          ),
          beginOffset = None,
          endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
          objectives = None,
          maxItemCount = None,
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

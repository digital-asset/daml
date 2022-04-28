// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
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

  it should "populate participant with create, consuming and non consuming exercises" in {

    val config = WorkflowConfig.FibonacciSubmissionConfig(
      numberOfInstances = 10,
      uniqueParties = false,
      value = 7,
    )

    for {
      ledgerApiServicesF <- LedgerApiServices.forChannel(
        channel = channel,
        authorizationHelper = None,
      )
      apiServices = ledgerApiServicesF("someUser")
      tested = CommandSubmitter(
        names = new Names(),
        benchtoolUserServices = apiServices,
        adminServices = apiServices,
        metricRegistry = new MetricRegistry,
        metricsManager = NoOpMetricsManager(),
      )
      (signatory, observers) <- tested.prepare(config)
      _ <- tested.submit(
        config = config,
        signatory = signatory,
        observers = observers,
        maxInFlightCommands = 1,
        submissionBatchSize = 5,
      )
      eventsObserver = EventsObserver(expectedTemplateNames =
        Set(
          com.daml.ledger.test.model.Bench.InefficientFibonacci.id
            .asInstanceOf[Identifier]
            .entityName
        )
      )
      _ <- apiServices.transactionService.transactionTrees(
        config = WorkflowConfig.StreamConfig.TransactionTreesStreamConfig(
          name = "dummy-name",
          filters = List(
            WorkflowConfig.StreamConfig.PartyFilter(
              party = signatory.toString,
              templates = List.empty,
            )
          ),
          beginOffset = None,
          endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
          objectives = None,
        ),
        observer = eventsObserver,
      )
      observerResult <- eventsObserver.result
    } yield {
      observerResult.createEvents.size shouldBe config.numberOfInstances withClue ("number of create events")
      succeed
    }
  }

}

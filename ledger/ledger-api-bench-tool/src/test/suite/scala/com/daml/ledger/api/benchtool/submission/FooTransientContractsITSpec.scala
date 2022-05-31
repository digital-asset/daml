// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.ConsumingExercises
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class FooTransientContractsITSpec
    extends AsyncFlatSpec
    with SandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues {

  it should "generate transient and non-transient contracts" in {

    val configTransient: WorkflowConfig.FooSubmissionConfig =
      makeSubmissionConfig(transientContracts = true, templateName = "Foo1")
    val configNonTransient = makeSubmissionConfig(transientContracts = false, templateName = "Foo2")

    for {
      ledgerApiServicesF <- LedgerApiServices.forChannel(
        channel = channel,
        authorizationHelper = None,
      )
      apiServices: LedgerApiServices = ledgerApiServicesF("someUser")
      submitter = CommandSubmitter(
        names = new Names(),
        benchtoolUserServices = apiServices,
        adminServices = apiServices,
        metricRegistry = new MetricRegistry,
        metricsManager = NoOpMetricsManager(),
        waitForSubmission = true,
      )
      // Parties are the same for 'transient' and 'non-transient' configs so we can use either one to allocate the parties
      allocatedParties <- submitter.prepare(configTransient)
      _ <- submit(
        apiServices = apiServices,
        config = configTransient,
        allocatedParties = allocatedParties,
      )
      _ <- submit(
        apiServices = apiServices,
        config = configNonTransient,
        allocatedParties = allocatedParties,
      )
      observerResult_transient: ObservedEvents <- flatEventsObserver(
        apiServices = apiServices,
        party = allocatedParties.signatory,
        templateName = FooTemplateDescriptor.forName("Foo1").fullyQualifiedName,
      )
      observerResult_nonTransient: ObservedEvents <- flatEventsObserver(
        apiServices = apiServices,
        party = allocatedParties.signatory,
        templateName = FooTemplateDescriptor.forName("Foo2").fullyQualifiedName,
      )
    } yield {
      observerResult_transient.allEventsSize shouldBe 0 withClue ("should see no transient contracts")
      observerResult_nonTransient.allEventsSize shouldBe 10 withClue ("should see all non-transient contracts")
      succeed
    }
  }

  private def submit(
      apiServices: LedgerApiServices,
      allocatedParties: AllocatedParties,
      config: WorkflowConfig.FooSubmissionConfig,
  ): Future[Unit] = {
    val submitter = CommandSubmitter(
      names = new Names(),
      benchtoolUserServices = apiServices,
      adminServices = apiServices,
      metricRegistry = new MetricRegistry,
      metricsManager = NoOpMetricsManager(),
      waitForSubmission = true,
    )
    new FooSubmission(
      submitter = submitter,
      maxInFlightCommands = 1,
      submissionBatchSize = 5,
      allocatedParties = allocatedParties,
      names = new Names(),
    ).performSubmission(submissionConfig = config)
  }

  private def makeSubmissionConfig(
      transientContracts: Boolean,
      templateName: String,
  ): WorkflowConfig.FooSubmissionConfig = {
    WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = 10,
      numberOfObservers = 0,
      numberOfDivulgees = 0,
      numberOfExtraSubmitters = 0,
      uniqueParties = true,
      instanceDistribution = List(
        WorkflowConfig.FooSubmissionConfig.ContractDescription(
          template = templateName,
          weight = 1,
          payloadSizeBytes = 0,
        )
      ),
      createAndConsumeInOneTransactionO = Some(transientContracts),
      nonConsumingExercises = None,
      consumingExercises = Some(
        ConsumingExercises(
          probability = 1.0,
          payloadSizeBytes = 100,
        )
      ),
      applicationIds = List.empty,
    )
  }

  private def flatEventsObserver(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
      templateName: String,
  ): Future[ObservedEvents] = {
    val eventsObserver = FlatEventsObserver(expectedTemplateNames = Set("Foo1", "Foo2"))
    val config = WorkflowConfig.StreamConfig.TransactionsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.toString,
          templates = List(templateName),
        )
      ),
      beginOffset = None,
      endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
      objectives = None,
      maxItemCount = None,
    )
    apiServices.transactionService.transactions(
      config = config,
      observer = eventsObserver,
    )
  }

}

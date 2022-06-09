// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class PartyAllocationITSpec
    extends AsyncFlatSpec
    with SandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues {

  it should "allow parties to be reused" in {

    val config = WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = 0,
      numberOfObservers = 1,
      numberOfDivulgees = 1,
      numberOfExtraSubmitters = 1,
      uniqueParties = false,
      instanceDistribution = Nil,
      nonConsumingExercises = None,
      consumingExercises = None,
      applicationIds = List.empty,
    )

    for {
      ledgerApiServicesF <- LedgerApiServices.forChannel(
        authorizationHelper = None,
        channel = channel,
      )
      apiServices = ledgerApiServicesF("someUser")
      submitter = CommandSubmitter(
        names = new Names(),
        benchtoolUserServices = apiServices,
        adminServices = apiServices,
        metricRegistry = new MetricRegistry,
        metricsManager = NoOpMetricsManager(),
        waitForSubmission = false,
      )
      parties1 <- submitter.prepare(config)
      parties2 <- submitter.prepare(config)
    } yield {
      parties1 shouldBe parties2
    }
  }

}

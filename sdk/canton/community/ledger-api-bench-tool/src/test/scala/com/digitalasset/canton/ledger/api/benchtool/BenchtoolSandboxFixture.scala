// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.EnvironmentSetup
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{
  CantonFixture,
  CantonFixtureAbstract,
  CantonFixtureIsolated,
}
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.*
import org.scalatest.Suite
import org.scalatest.time.{Minutes, Span}

import scala.concurrent.{ExecutionContext, Future}

trait AbstractBenchtoolSandboxFixture extends CantonFixtureAbstract {
  self: Suite & BaseTest & EnvironmentSetup =>

  override implicit lazy val patienceConfig: PatienceConfig = PatienceConfig(Span(2, Minutes))

  protected def benchtoolFixture()(implicit
      ec: ExecutionContext
  ): Future[(LedgerApiServices, Names, CommandSubmitter)] =
    for {
      ledgerApiServicesF <- LedgerApiServices.forChannel(
        channel = channel,
        authorizationHelper = None,
      )
      apiServices: LedgerApiServices = ledgerApiServicesF("someUser")
      names = new Names()
      submitter = CommandSubmitter(
        names = names,
        benchtoolUserServices = apiServices,
        adminServices = apiServices,
        metricsFactory = NoOpMetricsFactory,
        metricsManager = NoOpMetricsManager(),
        waitForSubmission = true,
        partyAllocating = new PartyAllocating(
          names = names,
          adminServices = apiServices,
        ),
        // Making command generation deterministic w.r.t. parallelism
        commandGenerationParallelism = 1,
        // Making command submission deterministic w.r.t. parallelism
        maxInFlightCommandsOverride = Some(1),
      )
    } yield (
      apiServices,
      names,
      submitter,
    )

  protected def benchtoolFooSubmissionFixture(
      submissionConfig: WorkflowConfig.FooSubmissionConfig
  )(implicit ec: ExecutionContext): Future[(LedgerApiServices, AllocatedParties, FooSubmission)] =
    for {
      (apiServices, _, submitter) <- benchtoolFixture()
      allocatedParties <- submitter.prepare(submissionConfig)
      foo = new FooSubmission(
        submitter = submitter,
        maxInFlightCommands = 1,
        submissionBatchSize = 1,
        allocatedParties = allocatedParties,
        names = new Names(),
        randomnessProvider = RandomnessProvider.forSeed(seed = 0),
      )
    } yield (apiServices, allocatedParties, foo)
}

trait BenchtoolSandboxFixture extends AbstractBenchtoolSandboxFixture with CantonFixture

trait BenchtoolSandboxFixtureIsolated
    extends AbstractBenchtoolSandboxFixture
    with CantonFixtureIsolated

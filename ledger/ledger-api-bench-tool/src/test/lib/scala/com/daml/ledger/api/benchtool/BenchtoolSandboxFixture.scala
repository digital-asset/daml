// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.submission.{
  AllocatedParties,
  CommandSubmitter,
  FooSubmission,
  Names,
  PartyAllocating,
  RandomnessProvider,
}
import com.daml.ledger.test.BenchtoolTestDar
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

trait BenchtoolSandboxFixture extends SandboxFixture {
  self: Suite =>

  override protected def packageFiles: List[File] = List(
    new File(rlocation(BenchtoolTestDar.path))
  )

  override def config = super.config.copy(
    engine = super.config.engine.copy(allowedLanguageVersions = LanguageVersion.EarlyAccessVersions)
  )

  def benchtoolFixture()(implicit
      ec: ExecutionContext
  ): Future[(LedgerApiServices, Names, CommandSubmitter)] = {
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
  }

  def benchtoolFooSubmissionFixture(
      submissionConfig: WorkflowConfig.FooSubmissionConfig
  )(implicit ec: ExecutionContext): Future[(LedgerApiServices, AllocatedParties, FooSubmission)] = {
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

}

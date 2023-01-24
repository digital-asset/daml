// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.{BenchtoolSandboxFixture, ConfigEnricher}
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.binding
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InterfaceSubscriptionITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues {

  it should "make interface subscriptions exposed to the benchtool" in {

    val foo1Config = WorkflowConfig.FooSubmissionConfig.ContractDescription(
      template = "Foo1",
      weight = 1,
      payloadSizeBytes = 100,
    )
    val foo2Config = WorkflowConfig.FooSubmissionConfig.ContractDescription(
      template = "Foo2",
      weight = 1,
      payloadSizeBytes = 100,
    )
    val foo3Config = WorkflowConfig.FooSubmissionConfig.ContractDescription(
      template = "Foo3",
      weight = 1,
      payloadSizeBytes = 100,
    )

    val config = WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = 100,
      numberOfObservers = 2,
      numberOfDivulgees = 0,
      numberOfExtraSubmitters = 0,
      uniqueParties = false,
      instanceDistribution = List(
        foo1Config,
        foo2Config,
        foo3Config,
      ),
      nonConsumingExercises = None,
      consumingExercises = None,
      applicationIds = List.empty,
    )

    for {
      (apiServices, names, submitter) <- benchtoolFixture()
      allocatedParties <- submitter.prepare(config)
      configDesugaring = new ConfigEnricher(
        allocatedParties,
        BenchtoolTestsPackageInfo.StaticDefault,
      )
      tested = new FooSubmission(
        submitter = submitter,
        maxInFlightCommands = 1,
        submissionBatchSize = 5,
        submissionConfig = config,
        allocatedParties = allocatedParties,
        names = names,
        partySelectingRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        payloadRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        consumingEventsRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        nonConsumingEventsRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        applicationIdRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        contractDescriptionRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
      )
      _ <- tested.performSubmission()
      observedEvents <- observer(
        configDesugaring = configDesugaring,
        apiServices = apiServices,
        party = allocatedParties.signatory,
      )
    } yield {
      observedEvents.createEvents.forall(_.interfaceViews.nonEmpty) shouldBe true
      observedEvents.createEvents
        .flatMap(_.interfaceViews)
        .forall(_.serializedSize > 0) shouldBe true
      observedEvents.createEvents
        .flatMap(_.interfaceViews)
        .map(_.interfaceName)
        .toSet shouldBe Set("FooI2", "FooI1", "FooI3")
    }
  }

  private def observer(
      configDesugaring: ConfigEnricher,
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
  ): Future[ObservedEvents] = {
    val config = WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.toString,
          templates = List.empty,
          interfaces = List("FooI2", "FooI1", "FooI3"),
        )
      ),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.activeContractsService.getActiveContracts(
      config = configDesugaring
        .enrichStreamConfig(config)
        .asInstanceOf[WorkflowConfig.StreamConfig.ActiveContractsStreamConfig],
      observer = ActiveContractsObserver(Set("Foo1", "Foo2", "Foo3")),
    )
  }

}

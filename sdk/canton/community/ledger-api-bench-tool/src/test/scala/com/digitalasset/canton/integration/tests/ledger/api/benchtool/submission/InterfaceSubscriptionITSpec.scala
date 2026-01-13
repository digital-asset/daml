// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  ActiveContractsObserver,
  BenchtoolTestsPackageInfo,
  ObservedEvents,
}
import com.digitalasset.canton.ledger.api.benchtool.{BenchtoolSandboxFixture, ConfigEnricher}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.AppendedClues

import scala.concurrent.{ExecutionContext, Future}

class InterfaceSubscriptionITSpec extends BenchtoolSandboxFixture with AppendedClues {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "interface subscriptions" should {
    "be exposed to the benchtool" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

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
        userIds = List.empty,
      )

      (for {
        (apiServices, allocatedParties, fooSubmission) <- benchtoolFooSubmissionFixture(config)
        configDesugaring = new ConfigEnricher(
          allocatedParties,
          BenchtoolTestsPackageInfo.StaticDefault,
        )
        _ <- fooSubmission.performSubmission(submissionConfig = config)
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
      }).futureValue
    }
  }

  private def observer(
      configDesugaring: ConfigEnricher,
      apiServices: LedgerApiServices,
      party: Party,
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = {
    val config = WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.getValue,
          templates = List.empty,
          interfaces = List("FooI2", "FooI1", "FooI3"),
        )
      ),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.stateService.getActiveContracts(
      config = configDesugaring
        .enrichStreamConfig(config)
        .asInstanceOf[WorkflowConfig.StreamConfig.ActiveContractsStreamConfig],
      observer = ActiveContractsObserver(Set("Foo1", "Foo2", "Foo3")),
    )
  }

}

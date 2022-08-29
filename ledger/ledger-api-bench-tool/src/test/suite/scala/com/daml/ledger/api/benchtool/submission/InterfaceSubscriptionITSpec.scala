// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.BenchtoolSandboxFixture
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.client.binding
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class InterfaceSubscriptionITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues {

  it should "populate participant with create, consuming and non consuming exercises" in {

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
      _ = allocatedParties.divulgees shouldBe empty
      tested = new FooSubmission(
        submitter = submitter,
        maxInFlightCommands = 1,
        submissionBatchSize = 5,
        submissionConfig = config,
        allocatedParties = allocatedParties,
        names = names,
      )
      _ <- tested.performSubmission()
      _ <- observer(
        apiServices = apiServices,
        party = allocatedParties.signatory,
      )
    } yield {
      succeed
    }
  }

  private def observer(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
  ): Future[Unit] = {
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
      timeoutInSecondsO = None,
    )
    apiServices.activeContractsService.getActiveContracts(
      config = config,
      observer = new ObserverWithResult[GetActiveContractsResponse, Unit](
        LoggerFactory.getLogger(getClass)
      ) {
        override def streamName: String = "???"

        override def completeWith(): Future[Unit] = Future.unit
      },
    )
  }

}

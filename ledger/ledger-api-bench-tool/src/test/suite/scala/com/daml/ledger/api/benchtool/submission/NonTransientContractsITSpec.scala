// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.BenchtoolSandboxFixture
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import com.daml.scalautil.Statement.discard
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Checkpoints, EitherValues, OptionValues}

import scala.concurrent.Future

class NonTransientContractsITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with EitherValues
    with OptionValues
    with Checkpoints {

  it should "submit non-transient contracts" in {
    val totalContractsCount = 100
    val submissionConfig = WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = totalContractsCount,
      numberOfObservers = 1,
      uniqueParties = false,
      allowNonTransientContracts = true,
      instanceDistribution = List(
        WorkflowConfig.FooSubmissionConfig.ContractDescription(
          template = "Foo1",
          weight = 1,
          payloadSizeBytes = 0,
        ),
        WorkflowConfig.FooSubmissionConfig.ContractDescription(
          template = "Foo2",
          weight = 1,
          payloadSizeBytes = 0,
        ),
      ),
      consumingExercises = Some(
        WorkflowConfig.FooSubmissionConfig.ConsumingExercises(
          probability = 0.7,
          payloadSizeBytes = 0,
        )
      ),
    )
    for {
      (apiServices, allocatedParties, submission) <- benchtoolFooSubmissionFixture(submissionConfig)
      _ <- submission.performSubmission(submissionConfig)
      txEvents: ObservedEvents <- txTreeObserver(
        apiServices = apiServices,
        party = allocatedParties.observers(0),
      )
    } yield {
      val createAndConsumeOffsetPairs = for {
        create <- txEvents.createEvents
        consume <- txEvents.consumingExercises.find(_.contractId == create.contractId).toList
      } yield create.offset -> consume.offset
      val activeContracts = txEvents.createEvents.count(create =>
        !txEvents.consumingExercises.exists(_.contractId == create.contractId)
      )
      val cp = new Checkpoint
      val nonTransientContracts = createAndConsumeOffsetPairs.count {
        case (createOffset, archiveOffset) => createOffset != archiveOffset
      }
      val transientContracts = createAndConsumeOffsetPairs.count {
        case (createOffset, archiveOffset) => createOffset == archiveOffset
      }
      cp(discard(nonTransientContracts shouldBe 47))
      cp(discard(transientContracts shouldBe 16))
      // sanity check:
      cp(
        discard(
          activeContracts + nonTransientContracts + transientContracts shouldBe totalContractsCount
        )
      )
      cp.reportAll()
      succeed
    }
  }

  private def txTreeObserver(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
      beginOffset: Option[LedgerOffset] = None,
  ): Future[ObservedEvents] = {
    val eventsObserver = TreeEventsObserver(expectedTemplateNames = Set("Foo1", "Foo2"))
    val config = WorkflowConfig.StreamConfig.TransactionTreesStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.toString,
          templates = List.empty,
          interfaces = List.empty,
        )
      ),
      beginOffset = beginOffset,
      endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.transactionService.transactionTrees(
      config = config,
      observer = eventsObserver,
    )
  }

}

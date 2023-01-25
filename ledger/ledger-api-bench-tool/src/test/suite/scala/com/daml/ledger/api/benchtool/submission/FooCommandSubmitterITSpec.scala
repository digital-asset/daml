// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.BenchtoolSandboxFixture
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import com.daml.scalautil.Statement.discard
import org.scalatest.{AppendedClues, Checkpoints}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class FooCommandSubmitterITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with Checkpoints {

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
    val consumingExercisesConfig = ConsumingExercises(
      probability = 1.0,
      payloadSizeBytes = 100,
    )
    val nonConsumingExercisesConfig = NonconsumingExercises(
      probability = 2.0,
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
      ),
      nonConsumingExercises = Some(nonConsumingExercisesConfig),
      consumingExercises = Some(consumingExercisesConfig),
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
        partySelectingRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        payloadRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        consumingEventsRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        nonConsumingEventsRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        applicationIdRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        contractDescriptionRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
      )
      _ <- tested.performSubmission()
      observerResult_signatory: ObservedEvents <- treeEventsObserver(
        apiServices = apiServices,
        party = allocatedParties.signatory,
      )
      observerResult_observer0: ObservedEvents <- treeEventsObserver(
        apiServices = apiServices,
        party = allocatedParties.observers(0),
      )
      observerResult_observer1: ObservedEvents <- treeEventsObserver(
        apiServices = apiServices,
        party = allocatedParties.observers(1),
      )
    } yield {
      val cp = new Checkpoint
      cp(
        discard(
          observerResult_signatory.createEvents.size shouldBe config.numberOfInstances withClue ("number of create events")
        )
      )
      cp(
        discard(
          observerResult_signatory.avgSizeOfCreateEventPerTemplateName(
            "Foo1"
          ) shouldBe 162 withClue ("payload size of create Foo1")
        )
      )
      cp(
        discard(
          observerResult_signatory.avgSizeOfCreateEventPerTemplateName(
            "Foo2"
          ) shouldBe 162 withClue ("payload size of create Foo2")
        )
      )
      cp(
        discard(
          observerResult_signatory.avgSizeOfConsumingExercise shouldBe 108
        )
      )
      cp(
        discard(
          observerResult_signatory.avgSizeOfNonconsumingExercise shouldBe 108
        )
      )
      cp(
        discard(
          observerResult_signatory.consumingExercises.size.toDouble shouldBe (config.numberOfInstances * consumingExercisesConfig.probability) withClue ("number of consuming exercises")
        )
      )
      val expectedNumberOfNonConsumingExercises =
        config.numberOfInstances * nonConsumingExercisesConfig.probability.toInt
      cp(
        discard(
          observerResult_signatory.nonConsumingExercises.size shouldBe expectedNumberOfNonConsumingExercises withClue ("number of non consuming exercises visible to signatory")
        )
      )
      // First observer can see all non-consuming events
      cp(
        discard(
          observerResult_observer0.nonConsumingExercises.size shouldBe expectedNumberOfNonConsumingExercises withClue ("number of non consuming exercises visible to Obs-0")
        )
      )
      // Second observer can see ~10% of all non-consuming events
      cp(
        discard(
          observerResult_observer1.nonConsumingExercises.size shouldBe 32 withClue ("number of non consuming exercises visible to Obs-1")
        )
      )
      cp.reportAll()
      succeed
    }
  }

  private def treeEventsObserver(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
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
      beginOffset = None,
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

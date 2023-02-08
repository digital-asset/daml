// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.config.{Config, WorkflowConfig}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner.Creator
import com.daml.ledger.api.benchtool.{BenchtoolSandboxFixture, PruningBenchmark}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Checkpoints, EitherValues, OptionValues}

import scala.concurrent.Future
import scala.concurrent.duration._

class PruningITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with EitherValues
    with OptionValues
    with Checkpoints {

  private var actorSystem: ActorSystem[SpawnProtocol.Command] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    actorSystem = ActorSystem(Creator(), "Creator")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    actorSystem.terminate()
  }

  it should "benchmark pruning" in {
    val submissionConfig = WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = 100,
      numberOfObservers = 1,
      uniqueParties = false,
      instanceDistribution = List(
        WorkflowConfig.FooSubmissionConfig.ContractDescription(
          template = "Foo1",
          weight = 1,
          payloadSizeBytes = 0,
        )
      ),
      consumingExercises = Some(
        WorkflowConfig.FooSubmissionConfig.ConsumingExercises(
          probability = 0.5,
          payloadSizeBytes = 0,
        )
      ),
    )
    val testedPruningBenchmark =
      new PruningBenchmark(reportingPeriod = Config.Default.reportingPeriod)
    for {
      (apiServices, allocatedParties, submission) <- benchtoolFooSubmissionFixture(submissionConfig)
      _ <- submission.performSubmission(submissionConfig)
      txPrePruning: ObservedEvents <- txTreeObserver(
        apiServices = apiServices,
        party = allocatedParties.observers(0),
      )
      pruningBenchmarkResult <- testedPruningBenchmark.benchmarkPruning(
        pruningConfig = WorkflowConfig.PruningConfig(
          name = "pruning-benchmark-test",
          pruneAllDivulgedContracts = true,
          maxDurationObjective = 0.nano,
        ),
        regularUserServices = apiServices,
        adminServices = apiServices,
        actorSystem = actorSystem,
        signatory = allocatedParties.signatory,
        names = new Names(),
      )
      acsPostPruning: ObservedEvents <- acsObserver(
        apiServices = apiServices,
        party = allocatedParties.observers(0),
      )
    } yield {
      pruningBenchmarkResult.value shouldBe ()
      txPrePruning.createEvents.size shouldBe 100
      txPrePruning.consumingExercises.size shouldBe 49
      acsPostPruning.numberOfCreatesPerTemplateName("Foo1") shouldBe 51
      acsPostPruning.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 0
      succeed
    }
  }

  private def txTreeObserver(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
      beginOffset: Option[LedgerOffset] = None,
  ): Future[ObservedEvents] = {
    val eventsObserver = TreeEventsObserver(expectedTemplateNames = Set("Foo1"))
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

  private def acsObserver(
      apiServices: LedgerApiServices,
      party: binding.Primitive.Party,
  ): Future[ObservedEvents] = {
    val eventsObserver = ActiveContractsObserver(expectedTemplateNames = Set("Foo1", "Foo2"))
    val config = WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.toString,
          templates = List.empty,
          interfaces = List.empty,
        )
      ),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.activeContractsService.getActiveContracts(
      config = config,
      observer = eventsObserver,
    )
  }

}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.config.{Config, WorkflowConfig}
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  ActiveContractsObserver,
  EventsObserver,
  Names,
  ObservedEvents,
}
import com.digitalasset.canton.ledger.api.benchtool.util.TypedActorSystemResourceOwner.Creator
import com.digitalasset.canton.ledger.api.benchtool.{BenchtoolSandboxFixture, PruningBenchmark}
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.actor.typed.{ActorSystem, SpawnProtocol}
import org.scalatest.{AppendedClues, Checkpoints, EitherValues, OptionValues}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class PruningITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with EitherValues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private var system: ActorSystem[SpawnProtocol.Command] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    system = ActorSystem(Creator(), "Creator")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  "benchtool" should {
    "benchmark pruning" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

      val reconciliationInterval: java.time.Duration = participant1.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .reconciliationInterval
        .asJava

      val maxDeduplicationDuration: java.time.Duration =
        participant1.config.init.ledgerApi.maxDeduplicationDuration.asJava

      val simClock = environment.simClock.value

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
      (for {
        (apiServices, allocatedParties, submission) <- benchtoolFooSubmissionFixture(
          submissionConfig
        )
        _ <- submission.performSubmission(submissionConfig)
        txPrePruning: ObservedEvents <- txObserver(
          apiServices = apiServices,
          party = allocatedParties.observers.head,
        )
        // advance the clock long enough to be sure that the last event to be pruned is followed by an acs commitment
        _ = simClock.advance(
          (if (maxDeduplicationDuration.compareTo(reconciliationInterval) >= 0)
             maxDeduplicationDuration
           else reconciliationInterval).multipliedBy(2)
        )
        pruningBenchmarkResult <- testedPruningBenchmark.benchmarkPruning(
          pruningConfig = WorkflowConfig.PruningConfig(
            name = "pruning-benchmark-test",
            pruneAllDivulgedContracts = true,
            maxDurationObjective = 0.nano,
          ),
          regularUserServices = apiServices,
          adminServices = apiServices,
          actorSystem = system,
          signatory = allocatedParties.signatory,
          names = new Names(),
        )
        acsPostPruning: ObservedEvents <- acsObserver(
          apiServices = apiServices,
          party = allocatedParties.observers.head,
        )
      } yield {
        pruningBenchmarkResult.value shouldBe ()
        txPrePruning.createEvents.size shouldBe 100
        txPrePruning.consumingExercises.size shouldBe 49
        acsPostPruning.numberOfCreatesPerTemplateName("Foo1") shouldBe 51
        acsPostPruning.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 0
        succeed
      }).futureValue
    }
  }

  private def txObserver(
      apiServices: LedgerApiServices,
      party: Party,
      beginOffsetExclusive: Long = 0L,
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = {
    val eventsObserver = EventsObserver(expectedTemplateNames = Set("Foo1"))
    for {
      ledgerEnd <- apiServices.stateService.getLedgerEnd()
      config = WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig(
        name = "dummy-name",
        filters = List(
          WorkflowConfig.StreamConfig.PartyFilter(
            party = party.getValue,
            templates = List.empty,
            interfaces = List.empty,
          )
        ),
        beginOffsetExclusive = beginOffsetExclusive,
        endOffsetInclusive = Some(ledgerEnd),
        objectives = None,
        maxItemCount = None,
        timeoutO = None,
      )
      txs <- apiServices.updateService.transactionsLedgerEffects(
        config = config,
        observer = eventsObserver,
      )
    } yield txs
  }

  private def acsObserver(
      apiServices: LedgerApiServices,
      party: Party,
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = {
    val eventsObserver = ActiveContractsObserver(expectedTemplateNames = Set("Foo1", "Foo2"))
    val config = WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.getValue,
          templates = List.empty,
          interfaces = List.empty,
        )
      ),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.stateService.getActiveContracts(
      config = config,
      observer = eventsObserver,
    )
  }

}

// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
}
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{EventsObserver, ObservedEvents}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.{AppendedClues, Checkpoints}

import scala.concurrent.{ExecutionContext, Future}

class FooCommandSubmitterITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "FooCommandSubmitter" should {
    "populate participant with create, consuming and non consuming exercises" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      env =>
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
          userIds = List.empty,
        )

        val (apiServices, allocatedParties, fooSubmission) =
          benchtoolFooSubmissionFixture(config).futureValue

        allocatedParties.divulgees shouldBe empty

        fooSubmission.performSubmission(submissionConfig = config).futureValue
        val observerResult_signatory = ledgerEffectsEventsObserver(
          apiServices = apiServices,
          party = allocatedParties.signatory,
        ).futureValue

        val observerResult_observer0 = ledgerEffectsEventsObserver(
          apiServices = apiServices,
          party = allocatedParties.observers.head,
        ).futureValue

        val observerResult_observer1 = ledgerEffectsEventsObserver(
          apiServices = apiServices,
          party = allocatedParties.observers(1),
        ).futureValue

        val cp = new Checkpoint
        cp(
          discard(
            observerResult_signatory.createEvents.size shouldBe config.numberOfInstances withClue "number of create events"
          )
        )
        cp(
          discard(
            observerResult_signatory.avgSizeOfCreateEventPerTemplateName(
              "Foo1"
            ) shouldBe 310 +- 20 withClue "payload size of create Foo1"
          )
        )
        cp(
          discard(
            observerResult_signatory.avgSizeOfCreateEventPerTemplateName(
              "Foo2"
            ) shouldBe 310 +- 20 withClue "payload size of create Foo2"
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
            observerResult_signatory.consumingExercises.size.toDouble shouldBe (config.numberOfInstances * consumingExercisesConfig.probability) withClue "number of consuming exercises"
          )
        )
        val expectedNumberOfNonConsumingExercises =
          config.numberOfInstances * nonConsumingExercisesConfig.probability.toInt
        cp(
          discard(
            observerResult_signatory.nonConsumingExercises.size shouldBe expectedNumberOfNonConsumingExercises withClue "number of non consuming exercises visible to signatory"
          )
        )
        // First observer can see all non-consuming events
        cp(
          discard(
            observerResult_observer0.nonConsumingExercises.size shouldBe expectedNumberOfNonConsumingExercises withClue "number of non consuming exercises visible to Obs-0"
          )
        )
        // Second observer can see ~10% of all non-consuming events (see probabilitiesByPartyIndex())
        cp(
          discard(
            observerResult_observer1.nonConsumingExercises.size shouldBe 14 withClue "number of non consuming exercises visible to Obs-1"
          )
        )
        cp.reportAll()
        succeed
    }
  }

  private def ledgerEffectsEventsObserver(
      apiServices: LedgerApiServices,
      party: Party,
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = for {
    ledgerEnd <- apiServices.stateService.getLedgerEnd()
    eventsObserver = EventsObserver(expectedTemplateNames = Set("Foo1", "Foo2"))
    config = WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig(
      name = "dummy-name",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.getValue,
          templates = List.empty,
          interfaces = List.empty,
        )
      ),
      beginOffsetExclusive = 0L,
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

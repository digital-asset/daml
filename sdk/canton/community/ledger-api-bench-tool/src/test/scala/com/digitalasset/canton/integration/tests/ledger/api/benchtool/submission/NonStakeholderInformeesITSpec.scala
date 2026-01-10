// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.ConsumingExercises
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{EventsObserver, ObservedEvents}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.{AppendedClues, Checkpoints, OptionValues}

import scala.concurrent.{ExecutionContext, Future}

class NonStakeholderInformeesITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "NonStakeholderInformees" should {
    "divulge events" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

      val expectedTemplateNames = Set("Foo1", "Divulger")
      val submissionConfig = WorkflowConfig.FooSubmissionConfig(
        numberOfInstances = 100,
        numberOfObservers = 1,
        numberOfDivulgees = 3,
        numberOfExtraSubmitters = 0,
        uniqueParties = false,
        instanceDistribution = List(
          WorkflowConfig.FooSubmissionConfig.ContractDescription(
            template = "Foo1",
            weight = 1,
            payloadSizeBytes = 0,
          )
        ),
        nonConsumingExercises = None,
        consumingExercises = Some(
          ConsumingExercises(
            probability = 0.1,
            payloadSizeBytes = 0,
          )
        ),
        userIds = List.empty,
      )
      (for {
        (apiServices, allocatedParties, fooSubmission) <- benchtoolFooSubmissionFixture(
          submissionConfig
        )
        _ <- fooSubmission.performSubmission(submissionConfig = submissionConfig)
        (ledgerEffectsResults_divulgee0, acsDeltaResults_divulgee0) <- observeAllTemplatesForParty(
          party = allocatedParties.divulgees.head,
          apiServices = apiServices,
          expectedTemplateNames = expectedTemplateNames,
        )
        (ledgerEffectsResults_divulgee1, acsDeltaResults_divulgee1) <- observeAllTemplatesForParty(
          party = allocatedParties.divulgees(1),
          apiServices = apiServices,
          expectedTemplateNames = expectedTemplateNames,
        )
        (ledgerEffectsResults_observer0, acsDeltaResults_observer0) <- observeAllTemplatesForParty(
          party = allocatedParties.observers.head,
          apiServices = apiServices,
          expectedTemplateNames = expectedTemplateNames,
        )
        (ledgerEffectsResults_signatory, _) <- observeAllTemplatesForParty(
          party = allocatedParties.signatory,
          apiServices = apiServices,
          expectedTemplateNames = expectedTemplateNames,
        )
      } yield {
        // Creates of Foo1 are divulged to "divulgee" party,
        // thus, they are visible on transaction ledger effects stream but absent from acs delta transactions stream.
        val cp = new Checkpoint

        {
          // Divulge0
          {
            // Create events
            val ledgerEffectsFoo1 =
              ledgerEffectsResults_divulgee0.numberOfCreatesPerTemplateName("Foo1")
            val acsDeltaFoo1 = acsDeltaResults_divulgee0.numberOfCreatesPerTemplateName("Foo1")
            cp(
              discard(
                ledgerEffectsFoo1 shouldBe 100 withClue "number of Foo1 contracts visible to divulgee0 on ledger effects transactions stream"
              )
            )
            cp(
              discard(
                acsDeltaFoo1 shouldBe 0 withClue "number of Foo1 contracts visible to divulgee0 on acs delta transactions stream"
              )
            )
            val divulger = ledgerEffectsResults_divulgee0.numberOfCreatesPerTemplateName("Divulger")
            // For 3 divulgees in total (a, b, c) there are 4 subsets that contain 'a': a, ab, ac, abc.
            cp(
              discard(
                divulger shouldBe 4 withClue "number of divulger contracts visible to divulgee0"
              )
            )
          }
          {
            // Consuming events (with 10% chance of generating a consuming event for a contract)
            val ledgerEffectsFoo1 =
              ledgerEffectsResults_divulgee0.numberOfConsumingExercisesPerTemplateName("Foo1")
            val acsDeltaFoo1 =
              acsDeltaResults_divulgee0.numberOfConsumingExercisesPerTemplateName("Foo1")
            cp(
              discard(
                ledgerEffectsFoo1 shouldBe 13 withClue "number of Foo1 consuming events visible to divulgee0 on ledger effects transactions stream"
              )
            )
            cp(
              discard(
                acsDeltaFoo1 shouldBe 0 withClue "number of Foo1 consuming events visible to divulgee0 on acs delta transactions stream"
              )
            )
          }
        }
        {
          // Divulgee1
          val ledgerEffectsFoo1 =
            ledgerEffectsResults_divulgee1.numberOfCreatesPerTemplateName("Foo1")
          val acsDeltaFoo1 = acsDeltaResults_divulgee1.numberOfCreatesPerTemplateName("Foo1")
          // This assertion will fail once in ~37k test executions with number of observed items being 0
          // because for 100 instances and 10% chance of divulging to divulgee1, divulgee1 won't be disclosed any contracts once in 1/(0.9**100) ~= 37649
          cp(discard(ledgerEffectsFoo1 shouldBe 9))
          cp(discard(acsDeltaFoo1 shouldBe 0))
          val divulger = ledgerEffectsResults_divulgee1.numberOfCreatesPerTemplateName("Divulger")
          cp(discard(divulger shouldBe 4))
        }
        {
          // Observer0
          val ledgerEffectsFoo1 =
            ledgerEffectsResults_observer0.numberOfCreatesPerTemplateName("Foo1")
          val acsDeltaFoo1 = acsDeltaResults_observer0.numberOfCreatesPerTemplateName("Foo1")
          cp(discard(ledgerEffectsFoo1 shouldBe 100))
          // Approximately 10% of contracts is created and archived in the same transaction and thus omitted from the acs delta transactions stream
          cp(discard(acsDeltaFoo1 shouldBe 87))
          val divulger = ledgerEffectsResults_observer0.numberOfCreatesPerTemplateName("Divulger")
          cp(discard(divulger shouldBe 0))
        }
        {
          val divulger = ledgerEffectsResults_signatory.numberOfCreatesPerTemplateName("Divulger")
          cp(discard(divulger shouldBe 7))
        }
        cp.reportAll()
        succeed
      }).futureValue
    }
  }

  private def observeAllTemplatesForParty(
      party: Party,
      apiServices: LedgerApiServices,
      expectedTemplateNames: Set[String],
  )(implicit ec: ExecutionContext): Future[(ObservedEvents, ObservedEvents)] = {
    val ledgerEffectsTxObserver = EventsObserver(expectedTemplateNames = expectedTemplateNames)
    val acsDeltaTxObserver = EventsObserver(expectedTemplateNames = expectedTemplateNames)
    for {
      ledgerEnd <- apiServices.stateService.getLedgerEnd()
      _ <- apiServices.updateService.transactionsLedgerEffects(
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
        ),
        observer = ledgerEffectsTxObserver,
      )
      ledgerEnd <- apiServices.stateService.getLedgerEnd()
      _ <- apiServices.updateService.transactions(
        config = WorkflowConfig.StreamConfig.TransactionsStreamConfig(
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
        ),
        observer = acsDeltaTxObserver,
      )
      ledgerEffectsResults: ObservedEvents <- ledgerEffectsTxObserver.result
      acsDeltaResults: ObservedEvents <- acsDeltaTxObserver.result
    } yield {
      (ledgerEffectsResults, acsDeltaResults)
    }
  }

}

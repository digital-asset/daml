// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{EventsObserver, ObservedEvents}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.{AppendedClues, Checkpoints, EitherValues, OptionValues}

import scala.concurrent.{ExecutionContext, Future}

class NonTransientContractsITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with EitherValues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  "benchtool" should {
    "submit non-transient contracts" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

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
      (for {
        (apiServices, allocatedParties, submission) <- benchtoolFooSubmissionFixture(
          submissionConfig
        )
        _ <- submission.performSubmission(submissionConfig)
        txEvents: ObservedEvents <- txObserver(
          apiServices = apiServices,
          party = allocatedParties.observers.head,
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
      }).futureValue
    }
  }

  private def txObserver(
      apiServices: LedgerApiServices,
      party: Party,
      beginOffsetExclusive: Long = 0,
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = for {
    ledgerEnd <- apiServices.stateService
      .getLedgerEnd()
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

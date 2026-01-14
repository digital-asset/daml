// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
  PartySet,
}
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyNamePrefixFilter,
  TransactionLedgerEffectsStreamConfig,
}
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  BenchtoolTestsPackageInfo,
  EventsObserver,
  ObservedEvents,
}
import com.digitalasset.canton.ledger.api.benchtool.{BenchtoolSandboxFixture, ConfigEnricher}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.{AppendedClues, Checkpoints, OptionValues}

import scala.concurrent.{ExecutionContext, Future}

class PartySetsITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "benchtool" should {
    "submit a party-set and apply party-set filter on a stream" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      env =>
        import env.*

        val submissionConfig = WorkflowConfig.FooSubmissionConfig(
          numberOfInstances = 10,
          numberOfObservers = 1,
          uniqueParties = false,
          instanceDistribution = List(
            WorkflowConfig.FooSubmissionConfig.ContractDescription(
              template = "Foo1",
              weight = 1,
              payloadSizeBytes = 0,
            )
          ),
          observerPartySets = List(
            PartySet(
              partyNamePrefix = "FooParty",
              count = 100,
              visibility = 0.5,
            ),
            PartySet(
              partyNamePrefix = "BarParty",
              count = 20,
              visibility = 0.05,
            ),
          ),
          consumingExercises = Some(
            ConsumingExercises(
              probability = 0.3,
              payloadSizeBytes = 0,
            )
          ),
          nonConsumingExercises = Some(
            NonconsumingExercises(
              probability = 2.0,
              payloadSizeBytes = 0,
            )
          ),
        )
        (for {
          (apiServices, allocatedParties, fooSubmission) <- benchtoolFooSubmissionFixture(
            submissionConfig
          )
          configDesugaring = new ConfigEnricher(
            allocatedParties,
            BenchtoolTestsPackageInfo.StaticDefault,
          )
          _ <- fooSubmission.performSubmission(submissionConfig = submissionConfig)
          _ = allocatedParties.observerPartySets
            .find(_.mainPartyNamePrefix == "FooParty")
            .value
            .parties(87)
            .getValue should startWith("FooParty-87")
          ledgerEffectsResults_fooParty87 <- observeStreams(
            configDesugaring = configDesugaring,
            filterByParties = List("FooParty-87"),
            apiServices = apiServices,
            expectedTemplateNames = Set("Foo1"),
          )
          ledgerEffectsResults_fooPartySet <- observeStreams(
            configDesugaring = configDesugaring,
            filterByPartyNamePrefixes = List("FooParty"),
            apiServices = apiServices,
            expectedTemplateNames = Set("Foo1"),
          )
          ledgerEffectsResults_fooPartyNamePrefix <- observeStreams(
            configDesugaring = configDesugaring,
            // matches 10 parties: {FooParty-30, FooParty-31, .., FooParty-39}
            filterByPartyNamePrefixes = List("FooParty-3"),
            apiServices = apiServices,
            expectedTemplateNames = Set("Foo1"),
          )
          ledgerEffectsResults_barPartySet <- observeStreams(
            configDesugaring = configDesugaring,
            filterByPartyNamePrefixes = List("BarParty"),
            apiServices = apiServices,
            expectedTemplateNames = Set("Foo1"),
          )
          ledgerEffectsResults_barPartyNamePrefix <- observeStreams(
            configDesugaring = configDesugaring,
            // Matches 10 parties: {BarParty-10, BarParty-11, .., BarParty-19}
            filterByPartyNamePrefixes = List("BarParty-1"),
            apiServices = apiServices,
            expectedTemplateNames = Set("Foo1"),
          )

        } yield {
          val cp = new Checkpoint

          { // Party from party set
            cp(
              discard(
                ledgerEffectsResults_fooParty87.numberOfCreatesPerTemplateName("Foo1") shouldBe 4
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooParty87.numberOfNonConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 8
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooParty87.numberOfConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 1
              )
            )
          }
          { // Foo party set
            cp(
              discard(
                ledgerEffectsResults_fooPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 10
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooPartySet.numberOfNonConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 20
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooPartySet.numberOfConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 4
              )
            )
          }
          { // Foo party set subset
            cp(
              discard(
                ledgerEffectsResults_fooPartyNamePrefix.numberOfCreatesPerTemplateName(
                  "Foo1"
                ) shouldBe 10
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooPartyNamePrefix
                  .numberOfNonConsumingExercisesPerTemplateName(
                    "Foo1"
                  ) shouldBe 20
              )
            )
            cp(
              discard(
                ledgerEffectsResults_fooPartyNamePrefix.numberOfConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 4
              )
            )
          }
          { // Bar party set
            cp(
              discard(
                ledgerEffectsResults_barPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 5
              )
            )
            cp(
              discard(
                ledgerEffectsResults_barPartySet.numberOfNonConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 10
              )
            )
            cp(
              discard(
                ledgerEffectsResults_barPartySet.numberOfConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 2
              )
            )
          }
          { // Bar party set subset
            cp(
              discard(
                ledgerEffectsResults_barPartyNamePrefix.numberOfCreatesPerTemplateName(
                  "Foo1"
                ) shouldBe 5
              )
            )
            cp(
              discard(
                ledgerEffectsResults_barPartyNamePrefix
                  .numberOfNonConsumingExercisesPerTemplateName(
                    "Foo1"
                  ) shouldBe 10
              )
            )
            cp(
              discard(
                ledgerEffectsResults_barPartyNamePrefix.numberOfConsumingExercisesPerTemplateName(
                  "Foo1"
                ) shouldBe 2
              )
            )
          }
          cp.reportAll()
          succeed
        }).futureValue
    }
  }

  private def observeStreams(
      configDesugaring: ConfigEnricher,
      filterByPartyNamePrefixes: List[String] = List.empty,
      filterByParties: List[String] = List.empty,
      filterByTemplates: List[String] = List.empty,
      apiServices: LedgerApiServices,
      expectedTemplateNames: Set[String],
  )(implicit ec: ExecutionContext): Future[ObservedEvents] = {
    val txObserver = EventsObserver(expectedTemplateNames = expectedTemplateNames)
    for {
      ledgerEnd <- apiServices.stateService.getLedgerEnd()
      ledgerEffectsConfig = TransactionLedgerEffectsStreamConfig(
        name = "dummy-name",
        filters = filterByParties.map(party =>
          WorkflowConfig.StreamConfig.PartyFilter(
            party = party,
            templates = filterByTemplates,
            interfaces = List.empty,
          )
        ),
        partyNamePrefixFilters = filterByPartyNamePrefixes.map(partyNamePrefix =>
          PartyNamePrefixFilter(
            partyNamePrefix = partyNamePrefix,
            templates = filterByTemplates,
          )
        ),
        beginOffsetExclusive = 0L,
        endOffsetInclusive = Some(ledgerEnd),
        objectives = None,
        maxItemCount = None,
        timeoutO = None,
      )
      _ <- apiServices.updateService.transactionsLedgerEffects(
        config = configDesugaring
          .enrichStreamConfig(ledgerEffectsConfig)
          .asInstanceOf[TransactionLedgerEffectsStreamConfig],
        observer = txObserver,
      )
      results: ObservedEvents <- txObserver.result
    } yield {
      results
    }
  }

}

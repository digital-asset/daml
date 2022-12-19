// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.{BenchtoolSandboxFixture, ConfigEnricher}
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.{
  ConsumingExercises,
  NonconsumingExercises,
  PartySet,
}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyNamePrefixFilter,
  TransactionTreesStreamConfig,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, OptionValues}

import scala.concurrent.Future

class PartySetsITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with OptionValues {

  it should "submit a party-set and apply party-set filter on a stream" in {
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
    for {
      (apiServices, names, submitter) <- benchtoolFixture()
      allocatedParties <- submitter.prepare(submissionConfig)
      configDesugaring = new ConfigEnricher(
        allocatedParties,
        BenchtoolTestsPackageInfo.StaticDefault,
      )
      tested = new FooSubmission(
        submitter = submitter,
        maxInFlightCommands = 1,
        submissionBatchSize = 1,
        submissionConfig = submissionConfig,
        allocatedParties = allocatedParties,
        names = names,
        partySelectingRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
        consumingEventsRandomnessProvider = RandomnessProvider.forSeed(seed = 0),
      )
      _ <- tested.performSubmission()
      _ = allocatedParties.observerPartySets
        .find(_.partyNamePrefix == "FooParty")
        .value
        .parties(87)
        .toString shouldBe "FooParty-87"
      treeResults_fooParty87 <- observeStreams(
        configDesugaring = configDesugaring,
        filterByParties = List("FooParty-87"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_fooPartySet <- observeStreams(
        configDesugaring = configDesugaring,
        filterByPartyNamePrefixO = Some("FooParty"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_barPartySet <- observeStreams(
        configDesugaring = configDesugaring,
        filterByPartyNamePrefixO = Some("BarParty"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )

    } yield {
      { // Party from party set
        treeResults_fooParty87.numberOfCreatesPerTemplateName("Foo1") shouldBe 6
        treeResults_fooParty87.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 12
        treeResults_fooParty87.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 1
      }
      { // Foo party set
        treeResults_fooPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 10
        treeResults_fooPartySet.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 20
        treeResults_fooPartySet.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 1
      }
      { // Bar party set
        treeResults_barPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 7
        treeResults_barPartySet.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 14
        treeResults_barPartySet.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 1
      }
      succeed
    }
  }

  private def observeStreams(
      configDesugaring: ConfigEnricher,
      filterByPartyNamePrefixO: Option[String] = None,
      filterByParties: List[String] = List.empty,
      filterByTemplates: List[String] = List.empty,
      apiServices: LedgerApiServices,
      expectedTemplateNames: Set[String],
  ): Future[ObservedEvents] = {
    val treeTxObserver = TreeEventsObserver(expectedTemplateNames = expectedTemplateNames)
    val treeConfig = TransactionTreesStreamConfig(
      name = "dummy-name",
      filters = filterByParties.map(party =>
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party,
          templates = filterByTemplates,
          interfaces = List.empty,
        )
      ),
      partyNamePrefixFilterO = filterByPartyNamePrefixO.map(partyNamePrefix =>
        PartyNamePrefixFilter(
          partyNamePrefix = partyNamePrefix,
          templates = filterByTemplates,
        )
      ),
      beginOffset = None,
      endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
      objectives = None,
      maxItemCount = None,
      timeoutInSecondsO = None,
    )
    for {
      _ <- apiServices.transactionService.transactionTrees(
        config = configDesugaring
          .enrichStreamConfig(treeConfig)
          .asInstanceOf[TransactionTreesStreamConfig],
        observer = treeTxObserver,
      )
      treeResults: ObservedEvents <- treeTxObserver.result
    } yield {
      treeResults
    }
  }

}

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
import com.daml.scalautil.Statement.discard
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Checkpoints, OptionValues}

import scala.concurrent.Future

class PartySetsITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with OptionValues
    with Checkpoints {

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
        .toString shouldBe "FooParty-87"
      treeResults_fooParty87 <- observeStreams(
        configDesugaring = configDesugaring,
        filterByParties = List("FooParty-87"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_fooPartySet <- observeStreams(
        configDesugaring = configDesugaring,
        filterByPartyNamePrefixes = List("FooParty"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_fooPartyNamePrefix <- observeStreams(
        configDesugaring = configDesugaring,
        // matches 10 parties: {FooParty-30, FooParty-31, .., FooParty-39}
        filterByPartyNamePrefixes = List("FooParty-3"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_barPartySet <- observeStreams(
        configDesugaring = configDesugaring,
        filterByPartyNamePrefixes = List("BarParty"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )
      treeResults_barPartyNamePrefix <- observeStreams(
        configDesugaring = configDesugaring,
        // Matches 10 parties: {BarParty-10, BarParty-11, .., BarParty-19}
        filterByPartyNamePrefixes = List("BarParty-1"),
        apiServices = apiServices,
        expectedTemplateNames = Set("Foo1"),
      )

    } yield {
      val cp = new Checkpoint

      { // Party from party set
        cp(discard(treeResults_fooParty87.numberOfCreatesPerTemplateName("Foo1") shouldBe 4))
        cp(
          discard(
            treeResults_fooParty87.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 8
          )
        )
        cp(
          discard(
            treeResults_fooParty87.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 1
          )
        )
      }
      { // Foo party set
        cp(discard(treeResults_fooPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 10))
        cp(
          discard(
            treeResults_fooPartySet.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 20
          )
        )
        cp(
          discard(
            treeResults_fooPartySet.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 4
          )
        )
      }
      { // Foo party set subset
        cp(
          discard(treeResults_fooPartyNamePrefix.numberOfCreatesPerTemplateName("Foo1") shouldBe 10)
        )
        cp(
          discard(
            treeResults_fooPartyNamePrefix.numberOfNonConsumingExercisesPerTemplateName(
              "Foo1"
            ) shouldBe 20
          )
        )
        cp(
          discard(
            treeResults_fooPartyNamePrefix.numberOfConsumingExercisesPerTemplateName(
              "Foo1"
            ) shouldBe 4
          )
        )
      }
      { // Bar party set
        cp(discard(treeResults_barPartySet.numberOfCreatesPerTemplateName("Foo1") shouldBe 5))
        cp(
          discard(
            treeResults_barPartySet.numberOfNonConsumingExercisesPerTemplateName("Foo1") shouldBe 10
          )
        )
        cp(
          discard(
            treeResults_barPartySet.numberOfConsumingExercisesPerTemplateName("Foo1") shouldBe 2
          )
        )
      }
      { // Bar party set subset
        cp(
          discard(treeResults_barPartyNamePrefix.numberOfCreatesPerTemplateName("Foo1") shouldBe 5)
        )
        cp(
          discard(
            treeResults_barPartyNamePrefix.numberOfNonConsumingExercisesPerTemplateName(
              "Foo1"
            ) shouldBe 10
          )
        )
        cp(
          discard(
            treeResults_barPartyNamePrefix.numberOfConsumingExercisesPerTemplateName(
              "Foo1"
            ) shouldBe 2
          )
        )
      }
      cp.reportAll()
      succeed
    }
  }

  private def observeStreams(
      configDesugaring: ConfigEnricher,
      filterByPartyNamePrefixes: List[String] = List.empty,
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
      partyNamePrefixFilters = filterByPartyNamePrefixes.map(partyNamePrefix =>
        PartyNamePrefixFilter(
          partyNamePrefix = partyNamePrefix,
          templates = filterByTemplates,
        )
      ),
      beginOffset = None,
      endOffset = Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
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

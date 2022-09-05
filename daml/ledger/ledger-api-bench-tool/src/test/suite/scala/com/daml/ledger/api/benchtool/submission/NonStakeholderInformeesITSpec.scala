// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.BenchtoolSandboxFixture
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.ConsumingExercises
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import org.scalatest.{AppendedClues, OptionValues}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class NonStakeholderInformeesITSpec
    extends AsyncFlatSpec
    with BenchtoolSandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with AppendedClues
    with OptionValues {

  it should "divulge events" in {
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
      applicationIds = List.empty,
    )
    for {
      (apiServices, names, submitter) <- benchtoolFixture()
      allocatedParties <- submitter.prepare(submissionConfig)
      tested = new FooSubmission(
        submitter = submitter,
        maxInFlightCommands = 1,
        submissionBatchSize = 5,
        submissionConfig = submissionConfig,
        allocatedParties = allocatedParties,
        names = names,
      )
      _ <- tested.performSubmission()
      (treeResults_divulgee0, flatResults_divulgee0) <- observeAllTemplatesForParty(
        party = allocatedParties.divulgees(0),
        apiServices = apiServices,
        expectedTemplateNames = expectedTemplateNames,
      )
      (treeResults_divulgee1, flatResults_divulgee1) <- observeAllTemplatesForParty(
        party = allocatedParties.divulgees(1),
        apiServices = apiServices,
        expectedTemplateNames = expectedTemplateNames,
      )
      (treeResults_observer0, flatResults_observer0) <- observeAllTemplatesForParty(
        party = allocatedParties.observers(0),
        apiServices = apiServices,
        expectedTemplateNames = expectedTemplateNames,
      )
      (treeResults_signatory, _) <- observeAllTemplatesForParty(
        party = allocatedParties.signatory,
        apiServices = apiServices,
        expectedTemplateNames = expectedTemplateNames,
      )
    } yield {
      // Creates of Foo1 are divulged to "divulgee" party,
      // thus, they are visible on transaction trees stream but absent from flat transactions stream.
      {
        // Divulge0
        {
          // Create events
          val treeFoo1 = treeResults_divulgee0.numberOfCreatesPerTemplateName("Foo1")
          val flatFoo1 = flatResults_divulgee0.numberOfCreatesPerTemplateName("Foo1")
          treeFoo1 shouldBe 100 withClue ("number of Foo1 contracts visible to divulgee0 on tree transactions stream")
          flatFoo1 shouldBe 0 withClue ("number of Foo1 contracts visible to divulgee0 on flat transactions stream")
          val divulger = treeResults_divulgee0.numberOfCreatesPerTemplateName("Divulger")
          // For 3 divulgees in total (a, b, c) there are 4 subsets that contain 'a': a, ab, ac, abc.
          divulger shouldBe 4 withClue ("number of divulger contracts visible to divulgee0")
        }
        {
          // Consuming events (with 10% chance of generating a consuming event for a contract)
          val treeFoo1 = treeResults_divulgee0.numberOfConsumingExercisesPerTemplateName("Foo1")
          val flatFoo1 = flatResults_divulgee0.numberOfConsumingExercisesPerTemplateName("Foo1")
          treeFoo1 should ((be > 0) and (be < submissionConfig.numberOfInstances / 5)) withClue ("number of Foo1 consuming events visible to divulgee0 on tree transactions stream")
          flatFoo1 shouldBe 0 withClue ("number of Foo1 consuming events visible to divulgee0 on flat transactions stream")
        }
      }
      {
        // Divulgee1
        val treeFoo1 = treeResults_divulgee1.numberOfCreatesPerTemplateName("Foo1")
        val flatFoo1 = flatResults_divulgee1.numberOfCreatesPerTemplateName("Foo1")
        // This assertion will fail once in ~37k test executions with number of observed items being 0
        // because for 100 instances and 10% chance of divulging to divulgee1, divulgee1 won't be disclosed any contracts once in 1/(0.9**100) ~= 37649
        treeFoo1 should ((be > 0) and (be < submissionConfig.numberOfInstances / 5))
        flatFoo1 shouldBe 0
        val divulger = treeResults_divulgee1.numberOfCreatesPerTemplateName("Divulger")
        divulger shouldBe 4
      }
      {
        // Observer0
        val treeFoo1 = treeResults_observer0.numberOfCreatesPerTemplateName("Foo1")
        val flatFoo1 = flatResults_observer0.numberOfCreatesPerTemplateName("Foo1")
        treeFoo1 shouldBe 100
        // Approximately 10% of contracts is created and archived in the same transaction and thus omitted from the flat transactions stream
        flatFoo1 should ((be > 70) and (be < submissionConfig.numberOfInstances))
        val divulger = treeResults_observer0.numberOfCreatesPerTemplateName("Divulger")
        divulger shouldBe 0
      }
      {
        val divulger = treeResults_signatory.numberOfCreatesPerTemplateName("Divulger")
        divulger shouldBe 7
      }
      succeed
    }
  }

  private def observeAllTemplatesForParty(
      party: binding.Primitive.Party,
      apiServices: LedgerApiServices,
      expectedTemplateNames: Set[String],
  ): Future[(ObservedEvents, ObservedEvents)] = {
    val treeTxObserver = TreeEventsObserver(expectedTemplateNames = expectedTemplateNames)
    val flatTxObserver = FlatEventsObserver(expectedTemplateNames = expectedTemplateNames)
    for {
      _ <- apiServices.transactionService.transactionTrees(
        config = WorkflowConfig.StreamConfig.TransactionTreesStreamConfig(
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
          timeoutInSecondsO = None,
        ),
        observer = treeTxObserver,
      )
      _ <- apiServices.transactionService.transactions(
        config = WorkflowConfig.StreamConfig.TransactionsStreamConfig(
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
          timeoutInSecondsO = None,
        ),
        observer = flatTxObserver,
      )
      treeResults: ObservedEvents <- treeTxObserver.result
      flatResults: ObservedEvents <- flatTxObserver.result
    } yield {
      (treeResults, flatResults)
    }
  }

}

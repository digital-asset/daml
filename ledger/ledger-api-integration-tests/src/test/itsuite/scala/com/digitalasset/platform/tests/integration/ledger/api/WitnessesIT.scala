// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.ExercisedEvent
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestIdsGenerator, TestTemplateIds}
import com.digitalasset.platform.participant.util.ValueConversions._
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class WitnessesIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers {
  override protected def config: Config = Config.default

  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds
  private val testIds = new TestIdsGenerator(config)

  private val filter = TransactionFilter(
    Map(
      Alice -> Filters.defaultInstance,
      Bob -> Filters.defaultInstance,
      Charlie -> Filters.defaultInstance,
    ))

  "The Ledger" should {
    "respect disclosure rules" in allFixtures { ctx =>
      val createArg = Record(
        fields = List(
          RecordField("p_signatory", Alice.asParty),
          RecordField("p_observer", Bob.asParty),
          RecordField("p_actor", Charlie.asParty)
        ))
      val exerciseArg = Value(Value.Sum.Record(Record()))

      def exercise(cid: String, choice: String): Future[ExercisedEvent] =
        ctx.testingHelpers
          .submitAndListenForSingleTreeResultOfCommand(
            ctx.testingHelpers
              .submitRequestWithId(
                testIds.testCommandId(s"witnesses-$choice-exercise-${UUID.randomUUID()}"),
                Charlie)
              .update(
                _.commands.commands :=
                  List(ExerciseCommand(Some(templateIds.witnesses), cid, choice, Some(exerciseArg)).wrap)
              ),
            filter,
            false
          )
          .map { tx =>
            tx.eventsById(tx.rootEventIds(0)).kind match {
              case TreeEvent.Kind.Exercised(e) => e
              case _ => fail("unexpected event")
            }
          }

      for {
        // Create Witnesses contract
        createTx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
          ctx.testingHelpers
            .submitRequestWithId(testIds.testCommandId("witnesses-create"), Alice)
            .update(
              _.commands.commands :=
                List(CreateCommand(Some(templateIds.witnesses), Some(createArg)).wrap)
            ),
          filter
        )
        createdEv = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(createTx))
        // Divulge Witnesses contract to charlie, who's just an actor and thus cannot
        // see it by default.
        divulgeCreatedEv <- ctx.testingHelpers.simpleCreate(
          testIds.testCommandId("witnesses-create-divulge"),
          Charlie,
          templateIds.divulgeWitnesses,
          Record(
            fields = List(RecordField(value = Alice.asParty), RecordField(value = Charlie.asParty)))
        )
        _ <- ctx.testingHelpers.simpleExercise(
          testIds.testCommandId("witnesses-exercise-divulge"),
          Alice,
          templateIds.divulgeWitnesses,
          divulgeCreatedEv.contractId,
          "Divulge",
          Value(
            Value.Sum.Record(
              Record(fields = List(RecordField(value = createdEv.contractId.asContractId)))))
        )
        // Now, first try the non-consuming choice
        nonConsumingExerciseEv <- exercise(createdEv.contractId, "WitnessesNonConsumingChoice")
        // And then the consuming one
        consumingExerciseEv <- exercise(createdEv.contractId, "WitnessesChoice")
      } yield {
        createdEv.witnessParties should contain theSameElementsAs List(Alice, Bob) // stakeholders = signatories \cup observers
        nonConsumingExerciseEv.witnessParties should contain theSameElementsAs List(Alice, Charlie) // signatories \cup actors
        consumingExerciseEv.witnessParties should contain theSameElementsAs List(
          Alice,
          Bob,
          Charlie) // stakeholders \cup actors
      }
    }
  }
}

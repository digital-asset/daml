// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import scala.concurrent.Future
import org.scalatest.{AsyncFreeSpec, Matchers}
import com.digitalasset.ledger.api.testing.utils.{
  SuiteResourceManagementAroundEach,
  AkkaBeforeAndAfterAll
}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import com.digitalasset.platform.apitesting.TestTemplateIds
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, LedgerContext}
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.ledger.api.v1.event.{ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.platform.testing.LedgerBackend.SandboxInMemory

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class WitnessIT
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with TestTemplateIds {
  override protected def config: Config = Config.default

  private def commandClient(ctx: LedgerContext): SynchronousCommandClient =
    new SynchronousCommandClient(ctx.commandService)

  private val filter = TransactionFilter(
    Map(
      "alice" -> Filters.defaultInstance,
      "bob" -> Filters.defaultInstance,
      "charlie" -> Filters.defaultInstance,
    ))

  // TODO test for all fixtures once <https://github.com/digital-asset/daml/issues/784> is fixed
  "disclosure rules are respected" in forAllMatchingFixtures {
    case TestFixture(SandboxInMemory, ctx) =>
      val createArg = Record(
        fields = List(
          RecordField("p_signatory", "alice".asParty),
          RecordField("p_observer", "bob".asParty),
          RecordField("p_actor", "charlie".asParty),
        ))
      val exerciseArg = Value(Value.Sum.Record(Record()))
      def exercise(cid: String, choice: String): Future[ExercisedEvent] =
        ctx.testingHelpers
          .submitAndListenForSingleTreeResultOfCommand(
            ctx.testingHelpers
              .submitRequestWithId(s"$choice-exercise")
              .update(
                _.commands.commands :=
                  List(ExerciseCommand(Some(templateIds.witnesses), cid, choice, Some(exerciseArg)).wrap),
                _.commands.party := "charlie"
              ),
            filter,
            false
          )
          .map { tx =>
            tx.eventsById(tx.rootEventIds(0)).kind match {
              case TreeEvent.Kind.Exercised(e) => e
              case _ => sys.error("unexpected event")
            }
          }
      for {
        // Create Witnesses contract
        createTx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
          ctx.testingHelpers
            .submitRequestWithId("create")
            .update(
              _.commands.commands :=
                List(CreateCommand(Some(templateIds.witnesses), Some(createArg)).wrap),
              _.commands.party := "alice"
            ),
          filter
        )
        createdEv = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(createTx))
        // Divulge Witnesses contract to charlie, who's just an actor and thus cannot
        // see it by default.
        divulgeCreatedEv <- ctx.testingHelpers.simpleCreate(
          "create-divulge",
          "charlie",
          templateIds.divulgeWitnesses,
          Record(
            fields =
              List(RecordField(value = "alice".asParty), RecordField(value = "charlie".asParty)))
        )
        _ <- ctx.testingHelpers.simpleExercise(
          "exercise-divulge",
          "alice",
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
        createdEv.witnessParties should contain theSameElementsAs List("alice", "bob") // stakeholders = signatories \cup observers
        nonConsumingExerciseEv.witnessParties should contain theSameElementsAs List(
          "alice",
          "charlie") // signatories \cup actors
        consumingExerciseEv.witnessParties should contain theSameElementsAs List(
          "alice",
          "bob",
          "charlie") // stakeholders \cup actors
      }
  }
}

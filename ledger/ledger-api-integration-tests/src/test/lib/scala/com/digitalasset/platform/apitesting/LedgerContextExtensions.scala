// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import akka.stream.ActorMaterializer
import com.digitalasset.ledger.api.testing.utils.MockMessages
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.Exercise
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.platform.tests.integration.ledger.api.LedgerTestingHelpers
import org.scalatest.{Assertion, Matchers, OptionValues}

import scala.concurrent.{ExecutionContext, Future}

object LedgerContextExtensions extends Matchers with OptionValues {

  implicit class ContextOps(val context: LedgerContext) extends AnyVal {

    import com.digitalasset.platform.participant.util.ValueConversions._

    def command(commandId: String, individualCommands: Seq[Command]): SubmitRequest =
      MockMessages.submitRequest.update(
        _.commands.commandId := commandId,
        _.commands.ledgerId := context.ledgerId,
        _.commands.commands := individualCommands)

    def testingHelpers(implicit mat: ActorMaterializer): LedgerTestingHelpers = {
      val commandClient = context.commandClient()

      new LedgerTestingHelpers(
        req => commandClient.flatMap(_.trackSingleCommand(req))(mat.executionContext),
        context
      )
    }

    def testingHelpers(configuration: CommandClientConfiguration)(
        implicit mat: ActorMaterializer): LedgerTestingHelpers = {
      val commandClient = context.commandClient(configuration = configuration)

      new LedgerTestingHelpers(
        req => commandClient.flatMap(_.trackSingleCommand(req))(mat.executionContext),
        context
      )
    }

    def submitExercise(
        commandId: String,
        template: Identifier,
        argument: Value,
        choice: String,
        contractId: String,
        submittingParty: String)(implicit mat: ActorMaterializer): Future[Transaction] = {
      testingHelpers.submitAndListenForSingleResultOfCommand(
        exerciseCommand(commandId, template, argument, choice, contractId, submittingParty),
        TransactionFilter(Map(submittingParty -> Filters.defaultInstance))
      )
    }

    def exerciseCommand(
        commandId: String,
        template: Identifier,
        argument: Value,
        choice: String,
        contractId: String,
        submittingParty: String): SubmitRequest = {
      command(
        commandId,
        List(Command(Exercise(ExerciseCommand(Some(template), contractId, choice, Some(argument)))))
      ).update(_.commands.party := submittingParty)
    }

    // Create a template instance and return the resulting create event.
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def submitCreate(
        commandId: String,
        template: Identifier,
        args: Seq[RecordField],
        submitter: String,
        verbose: Boolean = false)(
        implicit mat: ActorMaterializer,
        ec: ExecutionContext): Future[CreatedEvent] = {
      submitCreateWithListenerAndReturnEvent(
        commandId,
        template,
        args,
        submitter,
        submitter,
        Filters.defaultInstance,
        verbose)
    }

    // Create a template instance and return the resulting create event.
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def submitCreateWithListenerAndReturnEvent(
        commandId: String,
        template: Identifier,
        args: Seq[RecordField],
        submitter: String,
        listener: String,
        filters: Filters = Filters.defaultInstance,
        verbose: Boolean = false)(
        implicit mat: ActorMaterializer,
        ec: ExecutionContext): Future[CreatedEvent] = {

      for {
        tx <- submitCreateAndReturnTransaction(
          commandId,
          template,
          args,
          submitter,
          listener,
          filters,
          verbose)
      } yield {
        val helper = testingHelpers
        helper.archivedEventsIn(tx) shouldBe empty
        helper.getHead(helper.createdEventsIn(tx))
      }
    }

    def submitCreateAndReturnTransaction(
        commandId: String,
        template: Identifier,
        args: Seq[RecordField],
        submitter: String = MockMessages.party,
        listener: String = MockMessages.party,
        filters: Filters = Filters.defaultInstance,
        verbose: Boolean = false)(
        implicit mat: ActorMaterializer,
        ec: ExecutionContext): Future[Transaction] = {
      testingHelpers.submitAndListenForSingleResultOfCommand(
        createCommand(commandId, template, args, submitter),
        TransactionFilter(Map(listener -> filters)),
        verbose
      )
    }

    def createCommand(
        commandId: String,
        template: Identifier,
        args: Seq[RecordField],
        submitter: String) = {
      context
        .command(
          commandId,
          List(CreateCommand(Some(template), Some(Record(Some(template), args))).wrap))
        .update(_.commands.party := submitter)
    }

    // Create a template instance and verify that the listener can't see it
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def submitCreateWithListenerAndAssertNotVisible(
        commandId: String,
        template: Identifier,
        args: Seq[RecordField],
        submitter: String = MockMessages.party,
        listener: String = MockMessages.party,
        filters: Filters = Filters.defaultInstance)(
        implicit mat: ActorMaterializer,
        ec: ExecutionContext): Future[Assertion] = {
      withClue(s"Creation of a template instance should not be visible to the listener (for command ${commandId} using template ${template} on behalf of ${submitter})") {
        testingHelpers.submitAndVerifyFilterCantSeeResultOf(
          createCommand(commandId, template, args, submitter),
          TransactionFilter(Map(listener -> filters))
        )
      }
    }
  }

}

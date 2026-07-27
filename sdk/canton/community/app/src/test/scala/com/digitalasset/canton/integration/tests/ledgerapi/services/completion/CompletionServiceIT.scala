// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services.completion

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.commands.CreateCommand
import com.daml.ledger.api.v2.state_service.{GetLedgerEndRequest, StateServiceGrpc}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CantonConfig, DbConfig}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{CantonFixture, CreatesParties}
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.MockMessages
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

sealed trait CompletionServiceITBase extends CantonFixture with CreatesParties with TestCommands {

  private[this] val userId = "CompletionServiceIT"

  // How long it takes to download the entire completion stream.
  // Because the stream does not terminate, we use a timeout to determine when the stream
  // is done emitting elements.
  private[this] val completionTimeout = FiniteDuration(2, TimeUnit.SECONDS)

  private[this] def command(party: String) =
    CreateCommand(
      Some(templateIds.dummy),
      Some(
        Record(
          Some(templateIds.dummy),
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party))))),
        )
      ),
    ).wrap

  private[this] def submitAndWaitForTransactionRequest(party: String, commandId: String) =
    MockMessages.submitAndWaitForTransactionRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := commandId,
        _.commands.userId := userId,
        _.commands.actAs := Seq(party),
      )

  private[this] def submitAndWaitForOffset(
      commandService: CommandServiceGrpc.CommandServiceStub,
      party: String,
      commandId: String,
  )(implicit ec: ExecutionContext): Future[Long] =
    commandService
      .submitAndWaitForTransaction(submitAndWaitForTransactionRequest(party, commandId))
      .map(_.getTransaction.offset)

  private[this] def allCompletions(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      parties: Seq[String],
  )(implicit ec: ExecutionContext) =
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(userId, parties, 0),
        _,
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completionResponse.completion).map(_.commandId))

  private[this] def completionsFrom(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      parties: Seq[String],
      offset: Long,
  )(implicit ec: ExecutionContext) =
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(
          userId,
          parties,
          offset,
        ),
        _,
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completionResponse.completion).map(_.commandId))

  val partyHintA = "partyA"
  val partyHintB = "partyB"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, List(partyHintA, partyHintB))(directExecutionContext)
  }

  "CommandCompletionService" should {
    "return correct completions" in { env =>
      import env.*
      val commandService = CommandServiceGrpc.stub(channel)
      val completionService = CommandCompletionServiceGrpc.stub(channel)
      val stateService = StateServiceGrpc.stub(channel)

      val partyA = participant1.parties.find(partyHintA).toProtoPrimitive
      val partyB = participant1.parties.find(partyHintB).toProtoPrimitive

      val offset1 = submitAndWaitForOffset(commandService, partyA, "Cmd1").futureValue
      val offset2 = submitAndWaitForOffset(commandService, partyA, "Cmd2").futureValue
      val offset3 = submitAndWaitForOffset(commandService, partyB, "Cmd3").futureValue
      val offset4 = submitAndWaitForOffset(commandService, partyA, "Cmd4").futureValue
      val offset5 = submitAndWaitForOffset(commandService, partyB, "Cmd5").futureValue
      val offset6 = submitAndWaitForOffset(commandService, partyB, "Cmd6").futureValue

      val end = stateService.getLedgerEnd(GetLedgerEndRequest()).futureValue
      val allA = allCompletions(completionService, List(partyA)).futureValue
      val allB = allCompletions(completionService, List(partyB)).futureValue
      val halfA = completionsFrom(completionService, List(partyA), offset3).futureValue
      val halfB = completionsFrom(completionService, List(partyB), offset3).futureValue
      val emptyB = completionsFrom(completionService, List(partyB), offset6).futureValue

      end.offset should be >= offset6
      List(offset1, offset2, offset3, offset4, offset5, offset6).toSet.size shouldBe 6

      allA should contain theSameElementsInOrderAs List("Cmd1", "Cmd2", "Cmd4")
      allB should contain theSameElementsInOrderAs List("Cmd3", "Cmd5", "Cmd6")

      halfA should contain theSameElementsInOrderAs List("Cmd4")
      halfB should contain theSameElementsInOrderAs List("Cmd5", "Cmd6")

      emptyB shouldBe empty
    }
  }
}

final case class CompletionServiceOverrideConfig(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms.updateParticipantConfig("participant1") {
      _.focus(_.parameters.ledgerApiServer.indexer.inputMappingParallelism)
        .replace(NonNegativeInt.tryCreate(2))
        .focus(_.ledgerApi.authServices)
        .replace(Seq(Wildcard))
    }(config)
}

class CompletionServiceITPostgres extends CompletionServiceITBase {
  registerPlugin(CompletionServiceOverrideConfig(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

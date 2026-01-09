// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionResponse,
}
import com.daml.ledger.api.v2.commands.{Command, CreateCommand}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Transaction
import com.digitalasset.canton.damltests.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{CantonFixture, CreatesParties}
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.ledger.api.MockMessages
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

sealed trait SuperReaderUserIT extends CantonFixture with CreatesParties with TestCommands {

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

  private[this] def submitAndWaitRequest(party: String, commandId: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := commandId,
        _.commands.userId := "",
        _.commands.actAs := Seq(party),
      )

  private[this] def submitAndWait(
      commandService: CommandServiceGrpc.CommandServiceStub,
      party: String,
      commandId: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    commandService
      .submitAndWait(submitAndWaitRequest(party, commandId))
      .map(_.discard)

  private[this] def allCompletions(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      parties: Seq[String],
      userId: String = "test-user",
  )(implicit ec: ExecutionContext) =
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(
          userId = userId,
          parties = parties,
          beginExclusive = 0L,
        ),
        _,
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completionResponse.completion).map(_.commandId))

  val partyHintA = "partyA"
  val partyHintB = "partyB"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(asAdmin.token, List(partyHintA, partyHintB))(directExecutionContext)
  }

  "SuperReader" should {
    "get the completions for all parties" in { env =>
      import env.*

      val partyA = participant1.parties.find(partyHintA).toProtoPrimitive
      val partyB = participant1.parties.find(partyHintB).toProtoPrimitive

      val (_, actAsBothPartiesContext) =
        createUserByAdmin(
          "test-user",
          rights = Vector(
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(partyA))),
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(partyB))),
          ),
        ).futureValue
      val (_, superReaderContext) =
        createUserByAdmin(
          "user-that-reads-as-any-party",
          rights = Vector(
            proto.Right(proto.Right.Kind.CanReadAsAnyParty(proto.Right.CanReadAsAnyParty()))
          ),
        ).futureValue

      val commandService = stub(CommandServiceGrpc.stub(channel), actAsBothPartiesContext.token)
      def completionServiceWith(token: Option[String]) =
        stub(CommandCompletionServiceGrpc.stub(channel), token)

      submitAndWait(commandService, partyA, "Cmd1").futureValue
      submitAndWait(commandService, partyA, "Cmd2").futureValue
      submitAndWait(commandService, partyB, "Cmd3").futureValue
      submitAndWait(commandService, partyA, "Cmd4").futureValue
      submitAndWait(commandService, partyB, "Cmd5").futureValue
      submitAndWait(commandService, partyB, "Cmd6").futureValue

      val all =
        allCompletions(
          completionServiceWith(actAsBothPartiesContext.token),
          List(partyA, partyB),
        ).futureValue
      val allSuperReader =
        allCompletions(
          completionServiceWith(superReaderContext.token),
          List(partyA, partyB),
        ).futureValue

      allSuperReader should have size 6
      allSuperReader shouldBe all

    }

    "submit a command that requires a readAs right" in { env =>
      import env.*

      val partyA = participant1.parties.find(partyHintA).toProtoPrimitive
      val partyB = participant1.parties.find(partyHintB).toProtoPrimitive

      val (_, actAsBothPartiesContext) =
        createUserByAdmin(
          "super-user-" + UUID.randomUUID().toString,
          rights = Vector(
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(partyA))),
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(partyB))),
          ),
        ).futureValue

      val (_, superReaderContext) =
        createUserByAdmin(
          "user-that-acts-as-b-and-reads-as-any-party",
          rights = Vector(
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(partyB))),
            proto.Right(proto.Right.Kind.CanReadAsAnyParty(proto.Right.CanReadAsAnyParty())),
          ),
        ).futureValue

      def commandServiceWith(token: Option[String]) = stub(CommandServiceGrpc.stub(channel), token)
      val completionService =
        stub(CommandCompletionServiceGrpc.stub(channel), superReaderContext.token)

      val createCmd = new DivulgeIouByExercise(
        partyA,
        partyA,
      ).create.commands.asScala.toSeq
        .map(_.toProtoCommand)
        .map(Command.fromJavaProto)

      val createMsg = MockMessages.submitAndWaitForTransactionRequest
        .update(
          _.commands.commands := createCmd,
          _.commands.commandId := UUID.randomUUID().toString,
          _.commands.actAs := Seq(partyA),
          _.commands.userId := "",
          _.transactionFormat := TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(partyA -> Filters(Nil)),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          ),
        )

      val resp = commandServiceWith(actAsBothPartiesContext.token)
        .submitAndWaitForTransaction(createMsg)
        .futureValue

      val tx = Transaction.fromProto(
        SubmitAndWaitForTransactionResponse.toJavaProto(resp).getTransaction
      )

      val cid = JavaDecodeUtil.decodeAllCreated(DivulgeIouByExercise.COMPANION)(tx).loneElement.id

      def consumeSelfCmd = cid
        .exerciseConsumeSelf(partyB)
        .commands
        .asScala
        .toSeq
        .map(_.toProtoCommand)
        .map(Command.fromJavaProto)

      val req = MockMessages.submitAndWaitRequest
        .update(
          _.commands.commands := consumeSelfCmd,
          _.commands.userId := "",
          _.commands.actAs := Seq(partyB),
          _.commands.readAs := Seq(partyA),
        )

      commandServiceWith(superReaderContext.token)
        .submitAndWait(req)
        .map(_.discard)
        .futureValue

      val allSuperReader =
        allCompletions(
          completionService,
          List(partyB),
          "",
        ).futureValue

      allSuperReader should have size 1
    }
  }
}

class SuperReaderUserITDefault extends SuperReaderUserIT {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

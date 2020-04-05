// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.completion

import java.util.concurrent.TimeUnit

import com.daml.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.daml.platform.testing.StreamConsumer
import com.google.rpc.status.Status
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CompletionServiceIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundAll {

  private[this] val applicationId = "CompletionServiceIT"

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
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party)))))))).wrap

  private[this] def submitAndWaitRequest(ledgerId: String, party: String, commandId: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.ledgerId := ledgerId,
        _.commands.commandId := commandId,
        _.commands.applicationId := applicationId,
        _.commands.party := party,
      )

  private[this] def submitAndWaitForOffset(
      commandService: CommandServiceGrpc.CommandServiceStub,
      ledgerId: String,
      party: String,
      commandId: String,
  ): Future[String] =
    commandService
      .submitAndWaitForTransaction(submitAndWaitRequest(ledgerId, party, commandId))
      .map(_.getTransaction.offset)

  private[this] def allCompletions(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      ledgerId: String,
      parties: Seq[String],
  ) =
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(ledgerId, applicationId, parties, Some(MockMessages.ledgerBegin)),
        _
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completions).map(_.commandId))

  private[this] def completionsFrom(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      ledgerId: String,
      parties: Seq[String],
      offset: String,
  ) =
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(
          ledgerId,
          applicationId,
          parties,
          Some(LedgerOffset(LedgerOffset.Value.Absolute(offset)))),
        _
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completions).map(_.commandId))

  private[this] def ok(commandId: String) =
    Completion(commandId, Some(Status(0)))

  "CommandCompletionService" should {
    "return correct completions" in {
      val partyA = "partyA"
      val partyB = "partyB"
      val lid = ledgerId().unwrap
      val commandService = CommandServiceGrpc.stub(channel)
      val completionService = CommandCompletionServiceGrpc.stub(channel)

      for {
        offset1 <- submitAndWaitForOffset(commandService, lid, partyA, "Cmd1")
        offset2 <- submitAndWaitForOffset(commandService, lid, partyA, "Cmd2")
        offset3 <- submitAndWaitForOffset(commandService, lid, partyB, "Cmd3")
        offset4 <- submitAndWaitForOffset(commandService, lid, partyA, "Cmd4")
        offset5 <- submitAndWaitForOffset(commandService, lid, partyB, "Cmd5")
        offset6 <- submitAndWaitForOffset(commandService, lid, partyB, "Cmd6")

        end <- completionService.completionEnd(CompletionEndRequest(lid))
        allA <- allCompletions(completionService, lid, List(partyA))
        allB <- allCompletions(completionService, lid, List(partyB))
        halfA <- completionsFrom(completionService, lid, List(partyA), offset3)
        halfB <- completionsFrom(completionService, lid, List(partyB), offset3)
        emptyB <- completionsFrom(completionService, lid, List(partyB), offset6)
      } yield {
        end.offset shouldBe Some(LedgerOffset(LedgerOffset.Value.Absolute(offset6)))
        List(offset1, offset2, offset3, offset4, offset5, offset6).toSet.size shouldBe 6

        allA should contain theSameElementsInOrderAs List("Cmd1", "Cmd2", "Cmd4")
        allB should contain theSameElementsInOrderAs List("Cmd3", "Cmd5", "Cmd6")

        halfA should contain theSameElementsInOrderAs List("Cmd4")
        halfB should contain theSameElementsInOrderAs List("Cmd5", "Cmd6")

        emptyB shouldBe empty
      }
    }
  }

  override protected def config: SandboxConfig =
    super.config.copy(
      commandConfig = super.config.commandConfig.copy(
        inputBufferSize = 1,
        maxParallelSubmissions = 2,
        maxCommandsInFlight = 2
      )
    )

}

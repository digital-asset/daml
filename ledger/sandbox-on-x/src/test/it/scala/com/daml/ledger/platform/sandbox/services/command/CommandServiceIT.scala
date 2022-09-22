// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.command

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
  GetTimeModelResponse,
}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.fixture.{CreatesParties, SandboxFixture}
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.services.time.TimeProviderType
import com.google.protobuf.duration.{Duration => ProtoDuration}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._
import java.time.{Duration, Instant}
import java.util.UUID

sealed trait CommandServiceITBase
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxFixture
    with CreatesParties
    with TestCommands
    with SuiteResourceManagementAroundAll {

  private def command(party: String) =
    CreateCommand(
      Some(templateIds.dummy),
      Some(
        Record(
          Some(templateIds.dummy),
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party))))),
        )
      ),
    ).wrap

  private def submitAndWaitRequest(ledgerId: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(MockMessages.submitAndWaitRequest.getCommands.party)),
        _.commands.ledgerId := ledgerId,
        _.commands.commandId := UUID.randomUUID().toString,
      )

  private def submitRequest(ledgerId: String) =
    MockMessages.submitRequest
      .update(
        _.commands.commands := List(command(MockMessages.submitRequest.getCommands.party)),
        _.commands.ledgerId := ledgerId,
        _.commands.commandId := UUID.randomUUID().toString,
      )

  private[this] def assertExpectedDelay(
      start: Instant,
      end: Instant,
      minLedgerTimeRel: ProtoDuration,
      timeModel: GetTimeModelResponse,
  ) = {
    val avgLatency = DurationConversion.fromProto(timeModel.timeModel.get.avgTransactionLatency.get)
    val expectedDuration = DurationConversion.fromProto(minLedgerTimeRel).minus(avgLatency)
    val actualDuration = Duration.between(start, end)
    assert(
      actualDuration.compareTo(expectedDuration) != -1,
      s"Expected submission duration was $expectedDuration, actual duration way $actualDuration",
    )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, List(MockMessages.submitRequest.getCommands.party))
  }

  "CommandSubmissionService" when {
    "receiving a command with minLedgerTimeRel" should {
      "delay the submission" in {
        val lid = ledgerId().unwrap
        val submissionService = CommandSubmissionServiceGrpc.stub(channel)
        val configService = ConfigManagementServiceGrpc.stub(channel)
        val minLedgerTimeRel = ProtoDuration.of(5, 0)
        val request = submitRequest(lid).update(_.commands.minLedgerTimeRel := minLedgerTimeRel)

        for {
          timeModel <- configService.getTimeModel(GetTimeModelRequest())
          start = Instant.now
          _ <- submissionService.submit(request)
          end = Instant.now
        } yield {
          assertExpectedDelay(start, end, minLedgerTimeRel, timeModel)
        }
      }
    }
  }

  "CommandService" when {
    "receiving a command with minLedgerTimeRel" should {
      "delay the submission" in {
        val lid = ledgerId().unwrap
        val commandService = CommandServiceGrpc.stub(channel)
        val configService = ConfigManagementServiceGrpc.stub(channel)
        val minLedgerTimeRel = ProtoDuration.of(5, 0)
        val request =
          submitAndWaitRequest(lid).update(_.commands.minLedgerTimeRel := minLedgerTimeRel)

        for {
          timeModel <- configService.getTimeModel(GetTimeModelRequest())
          start = Instant.now
          _ <- commandService.submitAndWait(request)
          end = Instant.now
        } yield {
          assertExpectedDelay(start, end, minLedgerTimeRel, timeModel)
        }
      }
    }
  }

  override def config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.WallClock
      )
    )
  )

}

// CommandServiceIT on a Postgresql ledger
final class CommandServicePostgresIT extends CommandServiceITBase with SandboxBackend.Postgresql

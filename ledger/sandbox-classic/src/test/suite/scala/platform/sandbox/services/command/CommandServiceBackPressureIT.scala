// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.command

import java.util.UUID

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.testing.utils.{
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll,
}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import io.grpc.Status
import org.scalatest.{Assertion, Inspectors}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.util.{Failure, Success}

sealed trait CommandServiceBackPressureITBase
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundAll {

  private val commands = 50

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

  private def pushedBack(t: Throwable): Boolean = t match {
    case GrpcException(GrpcStatus.RESOURCE_EXHAUSTED(), _) => true
    case _ => false
  }

  private def testBackPressure[A](responses: Seq[Future[A]]): Future[Assertion] =
    Future
      .sequence(responses.map(_.map(Success(_)).recover({ case ex =>
        Failure(ex)
      })))
      .map(_.collect { case Failure(ex) => ex }) map { errors =>
      info(s"${errors.size}/$commands requests failed")
      info(s"${errors.count(pushedBack)}/${errors.size} errors are push-backs")
      errors should not be empty
      forAll(errors)(IsStatusException(Status.RESOURCE_EXHAUSTED))
    }

  "CommandSubmissionService" when {
    "overloaded with commands" should {
      "reject requests with RESOURCE_EXHAUSTED" in {
        val lid = ledgerId().unwrap
        val service = CommandSubmissionServiceGrpc.stub(channel)
        testBackPressure(Seq.fill(commands)(submitRequest(lid)).map(service.submit))
      }
    }
  }

  "CommandService" when {
    "overloaded with commands" should {
      "reject requests with RESOURCE_EXHAUSTED" in {
        val lid = ledgerId().unwrap
        val service = CommandServiceGrpc.stub(channel)
        testBackPressure(Seq.fill(commands)(submitAndWaitRequest(lid)).map(service.submitAndWait))
      }
    }
  }

  override protected def config: SandboxConfig =
    super.config.copy(
      commandConfig = super.config.commandConfig.copy(
        inputBufferSize = 1,
        maxCommandsInFlight = 2,
      ),
      maxParallelSubmissions = 2,
    )

}

// CommandServiceBackPressureIT on a Postgresql ledger
final class CommandServiceBackPressurePostgresIT
    extends CommandServiceBackPressureITBase
    with SandboxBackend.Postgresql

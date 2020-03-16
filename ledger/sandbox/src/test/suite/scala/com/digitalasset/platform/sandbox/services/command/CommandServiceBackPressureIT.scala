// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.command

import java.util.UUID

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.ledger.api.testing.utils.{
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresResource
import io.grpc.Status
import org.scalatest.{Assertion, AsyncWordSpec, Inspectors, Matchers}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandServiceBackPressureIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxFixture
    with TestCommands
    with SuiteResourceManagementAroundAll {

  private val commands = 50

  private def command(party: String) =
    CreateCommand(
      Some(templateIds.dummy),
      Some(
        Record(
          Some(templateIds.dummy),
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party)))))))).wrap

  private def submitAndWaitRequest(ledgerId: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(MockMessages.submitAndWaitRequest.getCommands.party)),
        _.commands.ledgerId := ledgerId,
        _.commands.commandId := UUID.randomUUID().toString
      )

  private def submitRequest(ledgerId: String) =
    MockMessages.submitRequest
      .update(
        _.commands.commands := List(command(MockMessages.submitRequest.getCommands.party)),
        _.commands.ledgerId := ledgerId,
        _.commands.commandId := UUID.randomUUID().toString
      )

  private def pushedBack(t: Throwable): Boolean = t match {
    case GrpcException(GrpcStatus.RESOURCE_EXHAUSTED(), _) => true
    case _ => false
  }

  private def testBackPressure[A](responses: Seq[Future[A]]): Future[Assertion] =
    Future
      .sequence(responses.map(_.map(Success(_)).recover({
        case ex => Failure(ex)
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

  override protected def database: Option[ResourceOwner[String]] =
    Some(PostgresResource.owner().map(_.jdbcUrl))

  override protected def config: SandboxConfig =
    super.config.copy(
      commandConfig = super.config.commandConfig.copy(
        inputBufferSize = 1,
        maxParallelSubmissions = 2,
        maxCommandsInFlight = 2
      )
    )

}

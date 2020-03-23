// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.command

import java.util.concurrent.atomic.AtomicInteger

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.util.control.NonFatal

final class CommandStaticTimeIT
    extends AsyncWordSpec
    with Matchers
    with TestCommands
    with SandboxFixture
    with ScalaFutures
    with SuiteResourceManagementAroundAll
    with OptionValues {

  private val newCommandId: () => String = {
    val atomicInteger = new AtomicInteger()
    () =>
      atomicInteger.incrementAndGet().toString
  }

  private lazy val unwrappedLedgerId = ledgerId().unwrap

  private def createCommandClient(): Future[CommandClient] =
    StaticTime
      .updatedVia(TimeServiceGrpc.stub(channel), unwrappedLedgerId)
      .recover { case NonFatal(_) => TimeProvider.UTC }(DirectExecutionContext)
      .map(tp =>
        new CommandClient(
          CommandSubmissionServiceGrpc.stub(channel),
          CommandCompletionServiceGrpc.stub(channel),
          ledgerId(),
          MockMessages.applicationId,
          CommandClientConfiguration(
            maxCommandsInFlight = 1,
            maxParallelSubmissions = 1,
            overrideTtl = true,
            ttl = java.time.Duration.ofSeconds(30)),
          None
        ).withTimeProvider(Some(tp)))(DirectExecutionContext)

  private lazy val submitRequest: SubmitRequest =
    MockMessages.submitRequest.update(
      _.commands.ledgerId := unwrappedLedgerId,
      _.commands.commands := List(
        CreateCommand(
          Some(templateIds.dummy),
          Some(
            Record(
              Some(templateIds.dummy),
              Seq(RecordField(
                "operator",
                Option(
                  Value(Value.Sum.Party(MockMessages.submitAndWaitRequest.commands.get.party)))))))
        ).wrap)
    )

  "Command and Time Services" when {

    "ledger effective time is within acceptance window" should {

      "commands should be accepted" in {
        for {
          commandClient <- createCommandClient()
          completion <- commandClient
            .withTimeProvider(None)
            .trackSingleCommand(
              SubmitRequest(
                Some(submitRequest.getCommands
                  .withLedgerId(unwrappedLedgerId)
                  .withCommandId(newCommandId()))))
        } yield {
          completion.status.value should have('code (Status.OK.getCode.value()))
        }
      }

    }
  }

}

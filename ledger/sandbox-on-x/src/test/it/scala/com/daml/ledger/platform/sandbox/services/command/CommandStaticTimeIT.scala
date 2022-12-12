// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.command

import java.util.concurrent.atomic.AtomicInteger

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.commands.CommandClient
import com.daml.ledger.client.services.testing.time.StaticTime
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.sandbox.fixture.{CreatesParties, SandboxFixture}
import com.daml.platform.sandbox.services.TestCommands
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class CommandStaticTimeIT
    extends AsyncWordSpec
    with Matchers
    with TestCommands
    with SandboxFixture
    with CreatesParties
    with ScalaFutures
    with SuiteResourceManagementAroundAll
    with OptionValues {

  private val newCommandId: () => String = {
    val atomicInteger = new AtomicInteger()
    () => atomicInteger.incrementAndGet().toString
  }

  private lazy val unwrappedLedgerId = ledgerId().unwrap

  private def createCommandClient()(implicit
      executionContext: ExecutionContext
  ): Future[CommandClient] =
    StaticTime
      .updatedVia(TimeServiceGrpc.stub(channel), unwrappedLedgerId)
      .recover { case NonFatal(_) => TimeProvider.UTC }
      .map(_ =>
        new CommandClient(
          CommandSubmissionServiceGrpc.stub(channel),
          CommandCompletionServiceGrpc.stub(channel),
          ledgerId(),
          MockMessages.applicationId,
          CommandClientConfiguration(
            maxCommandsInFlight = 1,
            maxParallelSubmissions = 1,
            defaultDeduplicationTime = java.time.Duration.ofSeconds(30),
          ),
        )
      )

  private lazy val submitRequest: SubmitRequest =
    MockMessages.submitRequest.update(
      _.commands.ledgerId := unwrappedLedgerId,
      _.commands.commands := List(
        CreateCommand(
          Some(templateIds.dummy),
          Some(
            Record(
              Some(templateIds.dummy),
              Seq(
                RecordField(
                  "operator",
                  Option(
                    Value(Value.Sum.Party(MockMessages.submitAndWaitRequest.commands.get.party))
                  ),
                )
              ),
            )
          ),
        ).wrap
      ),
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, List(MockMessages.submitAndWaitRequest.commands.get.party))
  }

  "Command and Time Services" when {

    "ledger effective time is within acceptance window" should {

      "commands should be accepted" in {
        for {
          commandClient <- createCommandClient()
          result <- commandClient
            .trackSingleCommand(
              SubmitRequest(
                Some(
                  submitRequest.getCommands
                    .withLedgerId(unwrappedLedgerId)
                    .withCommandId(newCommandId())
                )
              )
            )
        } yield {
          result shouldBe a[Right[_, _]]
        }
      }

    }
  }

}

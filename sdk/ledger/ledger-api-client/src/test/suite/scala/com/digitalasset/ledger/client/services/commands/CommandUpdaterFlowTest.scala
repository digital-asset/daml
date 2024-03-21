// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.{SubmissionIdGenerator, domain}
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.lf.data.Ref
import com.daml.telemetry.NoOpTelemetryContext
import com.daml.util.Ctx
import com.google.protobuf.duration.Duration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn
import scala.util.Failure

@nowarn("msg=deprecated")
class CommandUpdaterFlowTest extends AsyncWordSpec with Matchers with AkkaBeforeAndAfterAll {

  import CommandUpdaterFlowTest._

  "apply" should {
    "fail fast on an invalid ledger ID" in {
      val aCommandSubmission =
        CommandSubmission(defaultCommands.copy(ledgerId = "anotherLedgerId"))

      runCommandUpdaterFlow(aCommandSubmission)
        .transformWith {
          case Failure(exception) => exception shouldBe an[IllegalArgumentException]
          case _ => fail
        }
    }

    "fail fast on an invalid application ID" in {
      val aCommandSubmission =
        CommandSubmission(defaultCommands.copy(applicationId = "anotherApplicationId"))

      runCommandUpdaterFlow(aCommandSubmission)
        .transformWith {
          case Failure(exception) => exception shouldBe an[IllegalArgumentException]
          case _ => fail
        }
    }

    "generate a submission ID if it's empty" in {
      val aCommandSubmission = CommandSubmission(defaultCommands.copy(submissionId = ""))

      runCommandUpdaterFlow(aCommandSubmission)
        .map(_.value.commands.submissionId shouldBe aSubmissionId)
    }

    "set the default deduplication period if it's empty" in {
      val aCommandSubmission =
        CommandSubmission(
          defaultCommands.copy(deduplicationPeriod = DeduplicationPeriod.Empty)
        )
      val defaultDeduplicationTime = CommandClientConfiguration.default.defaultDeduplicationTime

      runCommandUpdaterFlow(aCommandSubmission)
        .map(
          _.value.commands.getDeduplicationTime shouldBe Duration
            .of(defaultDeduplicationTime.getSeconds, defaultDeduplicationTime.getNano)
        )
    }
  }

  private def runCommandUpdaterFlow(aCommandSubmission: CommandSubmission) = {
    Source
      .single(Ctx((), aCommandSubmission, NoOpTelemetryContext))
      .via(
        CommandUpdaterFlow(
          CommandClientConfiguration.default,
          aSubmissionIdGenerator,
          anApplicationId,
          aLedgerId,
        )
      )
      .runWith(Sink.head)
  }
}

object CommandUpdaterFlowTest {
  private val anApplicationId = "anApplicationId"
  private val aLedgerId = domain.LedgerId("aLedgerId")
  private val aSubmissionId = Ref.SubmissionId.assertFromString("aSubmissionId")
  private val aSubmissionIdGenerator: SubmissionIdGenerator = () => aSubmissionId
  private val defaultCommands =
    Commands.defaultInstance.copy(applicationId = anApplicationId, ledgerId = aLedgerId.toString)
}

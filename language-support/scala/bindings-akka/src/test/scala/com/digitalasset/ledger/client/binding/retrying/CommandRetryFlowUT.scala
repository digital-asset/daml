// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.retrying

import java.time.{Duration, Instant}

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.client.testing.AkkaTest
import com.digitalasset.ledger.client.binding.retrying.CommandRetryFlow.{
  In,
  Out,
  SubmissionFlowType
}
import com.digitalasset.util.Ctx
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.Code
import com.google.rpc.status.Status
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

class CommandRetryFlowUT extends AsyncWordSpec with Matchers with AkkaTest {

  /**
    * Uses the status received in the context for the first time,
    * then replies OK status as the ledger effective time is stepped.
    */
  val mockCommandSubmission: SubmissionFlowType[RetryInfo[Status]] =
    Flow[In[RetryInfo[Status]]]
      .map {
        case Ctx(
            context @ RetryInfo(_, _, _, status),
            SubmitRequest(Some(Commands(_, _, _, commandId, _, let, _, _)), tc)) =>
          if (let.get.seconds == 0) {
            Ctx(context, Completion(commandId, Some(status), tc))
          } else {
            Ctx(context, Completion(commandId, Some(status.copy(code = Code.OK_VALUE)), tc))
          }
        case x =>
          throw new RuntimeException(s"Unexpected input: '$x'")
      }

  private val timeProvider = TimeProvider.Constant(Instant.ofEpochSecond(60))
  private val maxRetryTime = Duration.ofSeconds(30)

  private def createRetry(retryInfo: RetryInfo[Status], completion: Completion) = {
    val commands = retryInfo.request.commands.get
    val let = commands.ledgerEffectiveTime.get
    val newLet = let.copy(seconds = let.seconds + 1)
    SubmitRequest(Some(commands.copy(ledgerEffectiveTime = Some(newLet))))
  }

  val retryFlow: SubmissionFlowType[RetryInfo[Status]] =
    CommandRetryFlow.createGraph(mockCommandSubmission, timeProvider, maxRetryTime, createRetry)

  private def submitRequest(statusCode: Int, time: Instant): Future[Seq[Out[RetryInfo[Status]]]] = {

    val request = SubmitRequest(
      Some(
        Commands(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "party",
          Some(Timestamp(0, 0)),
          Some(Timestamp(0, 0)),
          Seq.empty)),
      None
    )

    val input =
      Ctx(RetryInfo(request, 0, time, Status(statusCode, "message", Seq.empty)), request)

    Source
      .single(input)
      .via(retryFlow)
      .runWith(Sink.seq)
  }

  CommandRetryFlow.getClass.getSimpleName should {

    "propagete OK status" in {
      submitRequest(Code.OK_VALUE, Instant.ofEpochSecond(45)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 0
        result.head.value.status.get.code shouldBe Code.OK_VALUE
      }
    }

    "fail INVALID_ARGUMENT status" in {
      submitRequest(Code.INVALID_ARGUMENT_VALUE, Instant.ofEpochSecond(45)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 0
        result.head.value.status.get.code shouldBe Code.INVALID_ARGUMENT_VALUE
      }
    }

    "retry ABORTED status" in {
      submitRequest(Code.ABORTED_VALUE, Instant.ofEpochSecond(45)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 1
        result.head.value.status.get.code shouldBe Code.OK_VALUE
      }
    }

    "stop retrying after maxRetryTime" in {
      submitRequest(Code.ABORTED_VALUE, Instant.ofEpochSecond(15)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 0
        result.head.value.status.get.code shouldBe Code.ABORTED_VALUE
      }
    }

  }

}

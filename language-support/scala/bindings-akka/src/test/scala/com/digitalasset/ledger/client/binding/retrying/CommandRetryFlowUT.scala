// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.{Duration, Instant}

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.testing.AkkaTest
import com.daml.ledger.client.binding.retrying.CommandRetryFlow.{In, Out, SubmissionFlowType}
import com.daml.util.Ctx
import com.google.protobuf.duration.{Duration => protoDuration}
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
        case Ctx(context @ RetryInfo(_, _, _, status), SubmitRequest(Some(commands), tc)) =>
          if (commands.deduplicationTime.get.nanos == 0) {
            Ctx(context, Completion(commands.commandId, Some(status), traceContext = tc))
          } else {
            Ctx(
              context,
              Completion(
                commands.commandId,
                Some(status.copy(code = Code.OK_VALUE)),
                traceContext = tc))
          }
        case x =>
          throw new RuntimeException(s"Unexpected input: '$x'")
      }

  private val timeProvider = TimeProvider.Constant(Instant.ofEpochSecond(60))
  private val maxRetryTime = Duration.ofSeconds(30)

  private def createRetry(retryInfo: RetryInfo[Status], completion: Completion) = {
    val commands = retryInfo.request.commands.get
    val dedupTime = commands.deduplicationTime.get
    val newDedupTime = dedupTime.copy(nanos = dedupTime.nanos + 1)
    SubmitRequest(Some(commands.copy(deduplicationTime = Some(newDedupTime))))
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
          Seq.empty,
          Some(protoDuration.of(120, 0)),
        )
      ),
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

    "fail on all codes but RESOURCE_EXHAUSTED and UNAVAILABLE status" in {
      val codesToFail =
        Code
          .values()
          .toList
          .filterNot(c =>
            c == Code.UNRECOGNIZED || CommandRetryFlow.RETRYABLE_ERROR_CODES.contains(c.getNumber))
      val failedSubmissions = codesToFail.map { code =>
        submitRequest(code.getNumber, Instant.ofEpochSecond(45)) map { result =>
          result.size shouldBe 1
          result.head.context.nrOfRetries shouldBe 0
          result.head.value.status.get.code shouldBe code.getNumber
        }
      }
      Future.sequence(failedSubmissions).map(_ => succeed)
    }

    "retry RESOURCE_EXHAUSTED status" in {
      submitRequest(Code.RESOURCE_EXHAUSTED_VALUE, Instant.ofEpochSecond(45)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 1
        result.head.value.status.get.code shouldBe Code.OK_VALUE
      }
    }

    "retry UNAVAILABLE status" in {
      submitRequest(Code.UNAVAILABLE_VALUE, Instant.ofEpochSecond(45)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 1
        result.head.value.status.get.code shouldBe Code.OK_VALUE
      }
    }

    "stop retrying after maxRetryTime" in {
      submitRequest(Code.RESOURCE_EXHAUSTED_VALUE, Instant.ofEpochSecond(15)) map { result =>
        result.size shouldBe 1
        result.head.context.nrOfRetries shouldBe 0
        result.head.value.status.get.code shouldBe Code.RESOURCE_EXHAUSTED_VALUE
      }
    }

  }

}

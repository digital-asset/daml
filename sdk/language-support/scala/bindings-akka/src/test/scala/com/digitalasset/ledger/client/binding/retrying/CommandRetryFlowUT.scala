// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.{Duration, Instant}

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.binding.retrying.CommandRetryFlow.{In, Out, SubmissionFlowType}
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.NotOkResponse
import com.daml.ledger.client.testing.AkkaTest
import com.daml.util.Ctx
import com.google.rpc.Code
import com.google.rpc.status.Status
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class CommandRetryFlowUT extends AsyncWordSpec with Matchers with AkkaTest with Inside {

  /** Uses the status received in the context for the first time,
    * then replies OK status as the ledger effective time is stepped.
    */
  val mockCommandSubmission: SubmissionFlowType[RetryInfo[Status, CommandSubmission]] =
    Flow[In[RetryInfo[Status, CommandSubmission]]]
      .map {
        case Ctx(
              context @ RetryInfo(_, nrOfRetries, _, status),
              CommandSubmission(commands, _),
              _,
            ) =>
          // Return a completion based on the input status code only on the first submission.
          if (nrOfRetries == 0) {
            Ctx(
              context,
              CompletionResponse(
                completion = Completion(commands.commandId, Some(status)),
                checkpoint = None,
              ),
            )
          } else {
            Ctx(
              context,
              Right(
                CompletionResponse.CompletionSuccess(
                  completion = Completion(commands.commandId, Some(status)),
                  checkpoint = None,
                )
              ),
            )
          }
        case x =>
          throw new RuntimeException(s"Unexpected input: '$x'")
      }

  private val timeProvider = TimeProvider.Constant(Instant.ofEpochSecond(60))
  private val maxRetryTime = Duration.ofSeconds(30)

  val retryFlow: SubmissionFlowType[RetryInfo[Status, CommandSubmission]] =
    CommandRetryFlow.createGraph(mockCommandSubmission, timeProvider, maxRetryTime)

  private def submitRequest(
      statusCode: Int,
      time: Instant,
  ): Future[Seq[Out[RetryInfo[Status, CommandSubmission]]]] = {
    val request = CommandSubmission(
      Commands(
        ledgerId = "ledgerId",
        workflowId = "workflowId",
        applicationId = "applicationId",
        commandId = "commandId",
        party = "party",
        commands = Seq.empty,
      )
    )
    val input = Ctx(
      context = RetryInfo(
        value = request,
        nrOfRetries = 0,
        firstSubmissionTime = time,
        ctx = Status(statusCode, "message", Seq.empty),
      ),
      value = request,
    )
    Source.single(input).via(retryFlow).runWith(Sink.seq)
  }

  "command retry flow" should {

    "propagate OK status" in {
      submitRequest(Code.OK_VALUE, Instant.ofEpochSecond(45)) map { result =>
        inside(result) { case Seq(Ctx(context, Right(_), _)) =>
          context.nrOfRetries shouldBe 0
        }
      }
    }

    "fail on all codes but RESOURCE_EXHAUSTED and UNAVAILABLE status" in {
      val codesToFail =
        Code
          .values()
          .toList
          .filterNot(c =>
            c == Code.UNRECOGNIZED || CommandRetryFlow.RETRYABLE_ERROR_CODES
              .contains(c.getNumber) || c == Code.OK
          )
      val failedSubmissions = codesToFail.map { code =>
        submitRequest(code.getNumber, Instant.ofEpochSecond(45)) map { result =>
          inside(result) { case Seq(Ctx(context, Left(notOk: NotOkResponse), _)) =>
            context.nrOfRetries shouldBe 0
            notOk.grpcStatus.code shouldBe code.getNumber
          }
        }
      }
      Future.sequence(failedSubmissions).map(_ => succeed)
    }

    "retry RESOURCE_EXHAUSTED status" in {
      submitRequest(Code.RESOURCE_EXHAUSTED_VALUE, Instant.ofEpochSecond(45)) map { result =>
        inside(result) { case Seq(Ctx(context, Right(_), _)) =>
          context.nrOfRetries shouldBe 1
        }
      }
    }

    "retry UNAVAILABLE status" in {
      submitRequest(Code.UNAVAILABLE_VALUE, Instant.ofEpochSecond(45)) map { result =>
        inside(result) { case Seq(Ctx(context, Right(_), _)) =>
          context.nrOfRetries shouldBe 1
        }
      }
    }

    "stop retrying after maxRetryTime" in {
      submitRequest(Code.RESOURCE_EXHAUSTED_VALUE, Instant.ofEpochSecond(15)) map { result =>
        inside(result) { case Seq(Ctx(context, Left(notOk: NotOkResponse), _)) =>
          context.nrOfRetries shouldBe 0
          notOk.grpcStatus.code shouldBe Code.RESOURCE_EXHAUSTED_VALUE.intValue
        }
      }
    }

  }

}

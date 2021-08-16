// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CompletionResponse._
import com.daml.ledger.grpc.GrpcStatuses
import com.google.protobuf.any.Any
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.{ErrorInfo => JavaErrorInfo}
import com.google.rpc.status.Status
import io.grpc
import io.grpc.Status.Code
import io.grpc.Status.Code.OK
import io.grpc.protobuf
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompletionResponseTest extends AnyWordSpec with Matchers {

  "Completion response" when {

    val commandId = "commandId"
    val completion = Completion(
      commandId = commandId,
      status = Some(Status(OK.value(), "message", Seq(Any()))),
    )

    "convert to/from completion" should {

      "match successful completion" in {
        val completionWithTransactionId = completion.update(_.transactionId := "transactionId")
        val response = CompletionResponse(completionWithTransactionId)
        response shouldBe a[Right[_, _]]
        CompletionResponse.toCompletion(response) shouldEqual completionWithTransactionId
      }

      "match not ok status" in {
        val failedCodeCompletion = completion.update(_.status.code := Code.INTERNAL.value())
        val response =
          CompletionResponse(failedCodeCompletion)
        response should matchPattern { case Left(_: NotOkResponse) => }
        CompletionResponse.toCompletion(response) shouldEqual failedCodeCompletion
      }

      "handle missing status" in {
        val noStatusCodeCompletion = completion.update(_.optionalStatus := None)
        val response =
          CompletionResponse(noStatusCodeCompletion)
        response should matchPattern { case Left(_: NoStatusInResponse) => }
        CompletionResponse.toCompletion(response) shouldEqual noStatusCodeCompletion

      }

      "handle timeout" in {
        CompletionResponse.toCompletion(Left(TimeoutResponse(commandId))) shouldEqual Completion(
          commandId = commandId,
          status = Some(
            Status(
              code = Code.ABORTED.value(),
              message = "Timeout",
            )
          ),
        )
      }

    }

    "convert to exception" should {

      "convert queue completion failure" in {
        val exception =
          CompletionResponse.toException(QueueCompletionFailure(TimeoutResponse(commandId)))
        exception.getStatus.getCode shouldBe Code.ABORTED
      }

      "convert queue submit failure" in {
        val exception =
          CompletionResponse.toException(QueueSubmitFailure(grpc.Status.RESOURCE_EXHAUSTED))
        exception.getStatus.getCode shouldBe Code.RESOURCE_EXHAUSTED
      }

      "include metadata for status not ok" in {
        val errorInfo = ErrorInfo(
          metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "true")
        )
        val exception = CompletionResponse.toException(
          QueueCompletionFailure(
            NotOkResponse(
              commandId,
              Status(
                Code.CANCELLED.value(),
                details = Seq(
                  Any.pack(
                    errorInfo
                  )
                ),
              ),
            )
          )
        )
        val status = protobuf.StatusProto.fromThrowable(exception)
        val packedErrorInfo = status.getDetails(0).unpack(classOf[JavaErrorInfo])
        packedErrorInfo.getMetadataOrThrow(GrpcStatuses.DefiniteAnswerKey) shouldEqual "true"
      }

    }
  }
}

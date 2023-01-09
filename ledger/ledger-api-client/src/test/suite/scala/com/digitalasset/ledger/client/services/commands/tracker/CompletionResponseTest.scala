// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CompletionResponse._
import com.daml.ledger.grpc.GrpcStatuses
import com.google.protobuf.any.Any
import com.google.rpc.error_details.{ErrorInfo, RequestInfo}
import com.google.rpc.status.Status
import com.google.rpc.{ErrorInfo => JavaErrorInfo, RequestInfo => JavaRequestInfo}
import io.grpc
import io.grpc.Status.Code
import io.grpc.Status.Code.OK
import io.grpc.protobuf
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

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
        val response = CompletionResponse(completionWithTransactionId, None)
        response shouldBe a[Right[_, _]]
        CompletionResponse.toCompletion(response) shouldEqual completionWithTransactionId
      }

      "match not ok status" in {
        val failedCodeCompletion = completion.update(_.status.code := Code.INTERNAL.value())
        val response =
          CompletionResponse(failedCodeCompletion, None)
        response should matchPattern { case Left(_: NotOkResponse) => }
        CompletionResponse.toCompletion(response) shouldEqual failedCodeCompletion
      }

      "handle missing status" in {
        val noStatusCodeCompletion = completion.update(_.optionalStatus := None)
        val response =
          CompletionResponse(noStatusCodeCompletion, None)
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
      implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
        DamlContextualizedErrorLogger.forTesting(getClass)

      "convert queue completion failure" in {
        val exception =
          CompletionResponse.toException(
            QueueCompletionFailure(TimeoutResponse(commandId))
          )
        exception.getStatus.getCode shouldBe Code.DEADLINE_EXCEEDED
      }

      "convert queue submit failure" in {
        val status = GrpcStatus.buildStatus(
          Map.empty,
          GrpcStatus.toJavaBuilder(grpc.Status.RESOURCE_EXHAUSTED),
        )
        val exception =
          CompletionResponse.toException(
            QueueSubmitFailure(status)
          )
        exception.getStatus.getCode shouldBe Code.RESOURCE_EXHAUSTED
      }

      "include default metadata for status not ok" in {
        val exception = CompletionResponse.toException(
          QueueCompletionFailure(
            NotOkResponse(
              Completion(
                commandId = commandId,
                status = Some(
                  Status(
                    Code.CANCELLED.value(),
                    details = Seq.empty,
                  )
                ),
              ),
              None,
            )
          )
        )
        val status = protobuf.StatusProto.fromThrowable(exception)
        val packedErrorInfo = status.getDetails(0).unpack(classOf[JavaErrorInfo])
        packedErrorInfo.getMetadataOrThrow(GrpcStatuses.DefiniteAnswerKey) shouldEqual "false"
      }

      "include metadata for status not ok" in {
        val errorInfo = ErrorInfo(
          metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "true")
        )
        val exception = CompletionResponse.toException(
          QueueCompletionFailure(
            NotOkResponse(
              Completion(
                commandId = commandId,
                status = Some(
                  Status(
                    Code.CANCELLED.value(),
                    details = Seq(
                      Any.pack(
                        errorInfo
                      )
                    ),
                  )
                ),
              ),
              None,
            )
          )
        )
        val status = protobuf.StatusProto.fromThrowable(exception)
        val packedErrorInfo = status.getDetails(0).unpack(classOf[JavaErrorInfo])
        packedErrorInfo.getMetadataOrThrow(GrpcStatuses.DefiniteAnswerKey) shouldEqual "true"
      }

      "merge metadata for status not ok" in {
        val errorInfo = ErrorInfo(
          metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "true")
        )
        val requestInfo = RequestInfo(requestId = "aRequestId")
        val exception = CompletionResponse.toException(
          QueueCompletionFailure(
            NotOkResponse(
              Completion(
                commandId = commandId,
                status = Some(
                  Status(
                    Code.INTERNAL.value(),
                    details = Seq(
                      Any.pack(errorInfo),
                      Any.pack(requestInfo),
                    ),
                  )
                ),
              ),
              None,
            )
          )
        )

        val status = protobuf.StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.INTERNAL.value()
        val details = status.getDetailsList.asScala
        details.size shouldBe 2
        details.exists { detail =>
          detail.is(classOf[JavaErrorInfo]) && detail
            .unpack(classOf[JavaErrorInfo])
            .getMetadataOrThrow(GrpcStatuses.DefiniteAnswerKey) == "true"
        } shouldEqual true
        details.exists { detail =>
          detail.is(classOf[JavaRequestInfo]) && detail
            .unpack(classOf[JavaRequestInfo])
            .getRequestId == "aRequestId"
        } shouldEqual true
      }
    }
  }
}

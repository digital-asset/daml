// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  NoStatusInResponse,
  NotOkResponse,
  TimeoutResponse,
}
import com.google.protobuf.any.Any
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.Status.Code.OK
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
  }
}

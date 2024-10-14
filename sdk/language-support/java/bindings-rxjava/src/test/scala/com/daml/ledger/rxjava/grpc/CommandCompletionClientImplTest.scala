// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse.CompletionResponse
import com.daml.ledger.api.v2.completion.Completion
import com.google.rpc.status.Status
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import java.util.Optional

class CommandCompletionClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-completion-service-ledger")
  private val offset1 = 1L
  private val offset2 = 2L

  behavior of "[4.3] CommandCompletionClientImpl.completionStream"

  it should "return a stream with all the completions" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)), "1", offset = offset1)
    val completion2 = Completion("cid2", Option(new Status(1)), offset = offset2)

    val completionResponses = List(
      CompletionStreamResponse(CompletionResponse.Completion(completion1)),
      CompletionStreamResponse(CompletionResponse.Completion(completion2)),
    )
    ledgerServices.withCommandCompletionClient(
      completionResponses
    ) { (client, _) =>
      val completions = client
        .completionStream(applicationId, Optional.empty[java.lang.Long](), List("Alice").asJava)
        .take(2)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .iterator()

      val receivedCompletion1 = completions.next()
      val receivedCompletion2 = completions.next()
      receivedCompletion1.getCompletion.toScala.value.getOffset shouldBe offset1
      receivedCompletion1.getCompletion.toScala.value.getCommandId shouldBe completion1.commandId
      receivedCompletion1.getCompletion.toScala.value.getStatus.getCode shouldBe completion1.getStatus.code
      receivedCompletion1.getCompletion.toScala.value.getUpdateId shouldBe completion1.updateId
      receivedCompletion2.getCompletion.toScala.value.getOffset shouldBe offset2
      receivedCompletion2.getCompletion.toScala.value.getCommandId shouldBe completion2.commandId
      receivedCompletion2.getCompletion.toScala.value.getStatus.getCode shouldBe completion2.getStatus.code
    }
  }

  behavior of "[4.4] CommandCompletionClientImpl.completionStream"

  it should "send the request with the correct arguments" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)), offset = offset1)
    val completionResponse =
      CompletionStreamResponse(CompletionResponse.Completion(completion1))
    val parties = List("Alice")
    ledgerServices.withCommandCompletionClient(
      List(completionResponse)
    ) { (client, serviceImpl) =>
      client
        .completionStream(applicationId, Optional.empty[java.lang.Long](), parties.asJava)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      serviceImpl.getLastCompletionStreamRequest.value.applicationId shouldBe applicationId
      serviceImpl.getLastCompletionStreamRequest.value.beginExclusive shouldBe None
      serviceImpl.getLastCompletionStreamRequest.value.parties should contain theSameElementsAs parties
    }
  }

  behavior of "Authorization"

  def toAuthenticatedServer(fn: CommandCompletionClient => Any): Any = {
    val completion1 = Completion("cid1", Option(new Status(0)), offset = offset1)
    val completionResponse =
      CompletionStreamResponse(CompletionResponse.Completion(completion1))
    ledgerServices.withCommandCompletionClient(
      List(completionResponse),
      mockedAuthService,
    ) { (client, _) =>
      fn(client)
    }
  }

  it should "deny access without token" in {
    toAuthenticatedServer { client =>
      withClue("completionStream") {
        expectUnauthenticated {
          client
            .completionStream("appId", Optional.empty[java.lang.Long](), List(someParty).asJava)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingFirst()
        }
      }
      withClue("completionStream unbounded") {
        expectUnauthenticated {
          client
            .completionStream("appId", List(someParty).asJava)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingFirst()
        }
      }
    }
  }

  it should "deny access with the wrong token" in {
    toAuthenticatedServer { client =>
      withClue("completionStream") {
        expectPermissionDenied {
          client
            .completionStream(
              "appId",
              Optional.empty[java.lang.Long](),
              List(someParty).asJava,
              someOtherPartyReadToken,
            )
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingFirst()
        }
      }
      withClue("completionStream unbounded") {
        expectPermissionDenied {
          client
            .completionStream("appId", List(someParty).asJava, someOtherPartyReadToken)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingFirst()
        }
      }
    }
  }

  it should "allow access with the correct token" in {
    toAuthenticatedServer { client =>
      withClue("completionStream") {
        client
          .completionStream(
            "appId",
            Optional.empty[java.lang.Long](),
            List(someParty).asJava,
            somePartyReadToken,
          )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
      }
      withClue("completionStream unbounded") {
        client
          .completionStream("appId", List(someParty).asJava, somePartyReadToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
      }
    }
  }

}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit
import com.daml.ledger.api.v2.checkpoint.Checkpoint
import com.daml.ledger.javaapi.data.ParticipantOffset.{Absolute, ParticipantBegin}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.google.rpc.status.Status
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CommandCompletionClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-completion-service-ledger")
  private val offset1 = ParticipantOffset(ParticipantOffset.Value.Absolute("1"))
  private val offset2 = ParticipantOffset(ParticipantOffset.Value.Absolute("2"))

  behavior of "[4.3] CommandCompletionClientImpl.completionStream"

  it should "return a stream with all the completions" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)), "1")
    val completion2 = Completion("cid2", Option(new Status(1)))

    val completionResponses = List(
      CompletionStreamResponse(Some(Checkpoint(offset = Some(offset1))), Some(completion1)),
      CompletionStreamResponse(Some(Checkpoint(offset = Some(offset2))), Some(completion2)),
    )
    ledgerServices.withCommandCompletionClient(
      completionResponses
    ) { (client, _) =>
      val completions = client
        .completionStream(applicationId, ParticipantBegin.getInstance(), List("Alice").asJava)
        .take(2)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .iterator()

      val receivedCompletion1 = completions.next()
      val receivedCompletion2 = completions.next()
      receivedCompletion1.getCheckpoint.getOffset shouldBe new Absolute(offset1.getAbsolute)
      receivedCompletion1.getCompletion.getCommandId shouldBe completion1.commandId
      receivedCompletion1.getCompletion.getStatus.getCode shouldBe completion1.getStatus.code
      receivedCompletion1.getCompletion.getUpdateId shouldBe completion1.updateId
      receivedCompletion2.getCheckpoint.getOffset shouldBe new Absolute(offset2.getAbsolute)
      receivedCompletion2.getCompletion.getCommandId shouldBe completion2.commandId
      receivedCompletion2.getCompletion.getStatus.getCode shouldBe completion2.getStatus.code
    }
  }

  behavior of "[4.4] CommandCompletionClientImpl.completionStream"

  it should "send the request with the correct arguments" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)))
    val completionResponse =
      CompletionStreamResponse(Some(Checkpoint(offset = Some(offset1))), Some(completion1))
    val parties = List("Alice")
    ledgerServices.withCommandCompletionClient(
      List(completionResponse)
    ) { (client, serviceImpl) =>
      client
        .completionStream(applicationId, ParticipantBegin.getInstance(), parties.asJava)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      serviceImpl.getLastCompletionStreamRequest.value.applicationId shouldBe applicationId
      serviceImpl.getLastCompletionStreamRequest.value.getBeginExclusive.getAbsolute shouldBe "" // grpc default string is empty string
      serviceImpl.getLastCompletionStreamRequest.value.getBeginExclusive.getBoundary.isParticipantEnd shouldBe false
      serviceImpl.getLastCompletionStreamRequest.value.getBeginExclusive.getBoundary.isParticipantBegin shouldBe true
      serviceImpl.getLastCompletionStreamRequest.value.parties should contain theSameElementsAs parties
    }
  }

  behavior of "Authorization"

  def toAuthenticatedServer(fn: CommandCompletionClient => Any): Any = {
    val completion1 = Completion("cid1", Option(new Status(0)))
    val completionResponse =
      CompletionStreamResponse(Some(Checkpoint(offset = Some(offset1))), Some(completion1))
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
            .completionStream("appId", ParticipantBegin.getInstance(), List(someParty).asJava)
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
              ParticipantBegin.getInstance(),
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
            ParticipantBegin.getInstance(),
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

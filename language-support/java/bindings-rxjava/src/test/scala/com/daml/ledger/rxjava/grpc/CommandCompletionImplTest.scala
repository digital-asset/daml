// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.Optional
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.LedgerOffset
import com.daml.ledger.javaapi.data.LedgerOffset.LedgerBegin
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.completion.Completion
import com.google.rpc.status.Status
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.JavaConverters._

class CommandCompletionImplTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-completion-service-ledger")

  behavior of "[4.1] CommandCompletionClientImpl.getLedgerEnd"

  it should "return the ledger end" in {
    val offset = "offset"
    val response = genCompletionEndResponse(offset)
    ledgerServices.withCommandCompletionClient(List.empty, response) { (client, _) =>
      val end = client
        .completionEnd()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      end.getOffset shouldBe a[LedgerOffset.Absolute]
      end.getOffset.asInstanceOf[LedgerOffset.Absolute].getOffset shouldBe offset
    }
  }

  behavior of "[4.2] CommandCompletionClientImpl.completionEnd"

  it should "send the request with the correct ledgerId" in {
    ledgerServices.withCommandCompletionClient(List.empty, genCompletionEndResponse("")) {
      (client, serviceImpl) =>
        client
          .completionEnd()
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
        serviceImpl.getLastCompletionEndRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[4.3] CommandCompletionClientImpl.completionStream"

  it should "return a stream with all the completions" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)), "1", None)
    val completion2 = Completion("cid2", Option(new Status(1)), traceContext = None)
    val completionResponse = CompletionStreamResponse(None, List(completion1, completion2))
    ledgerServices.withCommandCompletionClient(
      List(completionResponse),
      genCompletionEndResponse("")) { (client, _) =>
      val completions = client
        .completionStream(applicationId, LedgerBegin.getInstance(), Set("Alice").asJava)
        .take(1)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .iterator()
        .next()
      completions.getCheckpoint shouldBe Optional.empty()
      completions.getCompletions.size() shouldBe 2
      val receivedCompletion1 = completions.getCompletions.get(0)
      val receivedCompletion2 = completions.getCompletions.get(1)
      receivedCompletion1.getCommandId shouldBe completion1.commandId
      receivedCompletion1.getStatus.getCode shouldBe completion1.getStatus.code
      receivedCompletion1.getTransactionId shouldBe completion1.transactionId
      receivedCompletion2.getCommandId shouldBe completion2.commandId
      receivedCompletion2.getStatus.getCode shouldBe completion2.getStatus.code
    }
  }

  behavior of "[4.4] CommandCompletionClientImpl.completionStream"

  it should "send the request with the correct ledgerId" in {
    val applicationId = "applicationId"
    val completion1 = Completion("cid1", Option(new Status(0)), traceContext = None)
    val completionResponse = CompletionStreamResponse(None, List(completion1))
    val parties = Set("Alice")
    ledgerServices.withCommandCompletionClient(
      List(completionResponse),
      genCompletionEndResponse("")) { (client, serviceImpl) =>
      client
        .completionStream(applicationId, LedgerBegin.getInstance(), parties.asJava)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      serviceImpl.getLastCompletionStreamRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      serviceImpl.getLastCompletionStreamRequest.value.applicationId shouldBe applicationId
      serviceImpl.getLastCompletionStreamRequest.value.getOffset.getAbsolute shouldBe "" // grpc default string is empty string
      serviceImpl.getLastCompletionStreamRequest.value.getOffset.getBoundary.isLedgerEnd shouldBe false
      serviceImpl.getLastCompletionStreamRequest.value.getOffset.getBoundary.isLedgerBegin shouldBe true
      serviceImpl.getLastCompletionStreamRequest.value.parties should contain theSameElementsAs parties
    }
  }
}

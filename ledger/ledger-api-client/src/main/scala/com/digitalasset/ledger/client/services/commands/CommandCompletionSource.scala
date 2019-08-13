// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest,
  CompletionStreamResponse
}
import io.grpc.stub.StreamObserver

import scala.collection.{breakOut, immutable}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object CommandCompletionSource {

  def toStreamElements(
      response: CompletionStreamResponse): immutable.Iterable[CompletionStreamElement] = {

    val completions: Vector[CompletionStreamElement] =
      response.completions.map(CompletionStreamElement.CompletionElement)(breakOut)
    response.checkpoint.fold(completions)(cp =>
      completions :+ CompletionStreamElement.CheckpointElement(cp))
  }

  def apply(
      request: CompletionStreamRequest,
      stub: (CompletionStreamRequest, StreamObserver[CompletionStreamResponse]) => Unit)(
      implicit esf: ExecutionSequencerFactory): Source[CompletionStreamElement, NotUsed] = {
    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(toStreamElements)
      .log(
        "completion at client", {
          case CompletionStreamElement.CheckpointElement(c) => s"Checkpoint ${c.offset}"
          case CompletionStreamElement.CompletionElement(c) => s"Completion $c"
        }
      )
  }
}

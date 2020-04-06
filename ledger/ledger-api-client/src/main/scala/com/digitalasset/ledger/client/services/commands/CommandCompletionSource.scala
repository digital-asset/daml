// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.ledger.api.v1.command_completion_service.{
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

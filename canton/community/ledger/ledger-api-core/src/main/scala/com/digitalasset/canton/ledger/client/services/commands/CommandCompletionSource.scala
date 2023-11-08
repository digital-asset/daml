// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.completion.Completion
import io.grpc.stub.StreamObserver

import scala.collection.immutable

object CommandCompletionSource {

  def toStreamElements(
      response: CompletionStreamResponse
  ): immutable.Iterable[CompletionStreamElement] = {

    val completions: Vector[CompletionStreamElement] =
      response.completions.view
        .map((completion: Completion) =>
          CompletionStreamElement.CompletionElement(completion, response.checkpoint)
        )
        .toVector
    response.checkpoint.fold(completions)(cp =>
      completions :+ CompletionStreamElement.CheckpointElement(cp)
    )
  }

  def apply(
      request: CompletionStreamRequest,
      stub: (CompletionStreamRequest, StreamObserver[CompletionStreamResponse]) => Unit,
  )(implicit esf: ExecutionSequencerFactory): Source[CompletionStreamElement, NotUsed] = {
    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(toStreamElements)
  }
}

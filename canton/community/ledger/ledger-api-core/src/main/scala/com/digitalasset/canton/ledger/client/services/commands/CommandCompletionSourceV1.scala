// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.completion.Completion
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.collection.immutable

object CommandCompletionSourceV1 {

  def toStreamElements(
      response: CompletionStreamResponse
  ): immutable.Iterable[CompletionStreamElementV1] = {

    val completions: Vector[CompletionStreamElementV1] =
      response.completions.view
        .map((completion: Completion) =>
          CompletionStreamElementV1.CompletionElement(completion, response.checkpoint)
        )
        .toVector
    response.checkpoint.fold(completions)(cp =>
      completions :+ CompletionStreamElementV1.CheckpointElement(cp)
    )
  }

  def apply(
      request: CompletionStreamRequest,
      stub: (CompletionStreamRequest, StreamObserver[CompletionStreamResponse]) => Unit,
  )(implicit esf: ExecutionSequencerFactory): Source[CompletionStreamElementV1, NotUsed] = {
    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(toStreamElements)
  }
}

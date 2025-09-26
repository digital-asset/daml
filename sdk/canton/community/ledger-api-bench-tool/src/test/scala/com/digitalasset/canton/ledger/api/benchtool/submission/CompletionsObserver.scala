// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object CompletionsObserver {
  def apply(): CompletionsObserver = new CompletionsObserver(
    logger = LoggerFactory.getLogger(getClass)
  )
}

class CompletionsObserver(logger: Logger)
    extends ObserverWithResult[CompletionStreamResponse, ObservedCompletions](logger) {

  private val completions = collection.mutable.ArrayBuffer[ObservedCompletion]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: CompletionStreamResponse): Unit =
    for {
      completion <- value.completionResponse.completion
    } {
      completions.addOne(
        ObservedCompletion(
          userId = completion.userId,
          actAs = completion.actAs,
        )
      )
    }

  override def completeWith(): Future[ObservedCompletions] = Future.successful(
    ObservedCompletions(
      completions = completions.toList
    )
  )
}

final case class ObservedCompletion(userId: String, actAs: Seq[String])

final case class ObservedCompletions(
    completions: Seq[ObservedCompletion]
)

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
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

  override def onNext(value: CompletionStreamResponse): Unit = {
    for {
      completion <- value.completions
    } {
      completions.addOne(
        ObservedCompletion(
          applicationId = completion.applicationId,
          actAs = completion.actAs,
        )
      )
    }
  }

  override def completeWith(): Future[ObservedCompletions] = Future.successful(
    ObservedCompletions(
      completions = completions.toList
    )
  )
}

case class ObservedCompletion(applicationId: String, actAs: Seq[String])

case class ObservedCompletions(
    completions: Seq[ObservedCompletion]
)

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import ledger.offset.Offset
import ledger.participant.state.v2.Update
import ledger.resources.ResourceOwner
import logging.{ContextualizedLogger, LoggingContext}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}

import scala.util.chaining._

object AkkaSubmissionsBridge {
  private val logger = ContextualizedLogger.get(getClass)
  def apply()(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ): ResourceOwner[(Sink[(Offset, Update), NotUsed], Source[(Offset, Update), NotUsed])] =
    ResourceOwner.forValue(() => {
      MergeHub
        // We can't instrument these buffers, therefore keep these to minimal sizes and
        // use a configurable instrumented buffer in the producer.
        .source[(Offset, Update)](perProducerBufferSize = 1)
        .toMat(BroadcastHub.sink(bufferSize = 1))(Keep.both)
        .run()
        .tap { _ =>
          logger.info("Instantiated Akka submissions bridge.")
        }
    })
}

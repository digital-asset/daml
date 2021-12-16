// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.{
  Configuration,
  LedgerId,
  LedgerInitialConditions,
  LedgerTimeModel,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}

import scala.util.chaining._

trait ReadServiceWithFeedSink extends ReadService {
  def feedSink: Sink[(Offset, Update), NotUsed]
}

object ReadServiceWithFeedSink {
  def apply(
      ledgerId: LedgerId,
      maxDedupSeconds: Int,
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ) = new ReadServiceWithFeedSinkImpl(ledgerId, maxDedupSeconds)

  class ReadServiceWithFeedSinkImpl private[ReadServiceWithFeedSink] (
      ledgerId: LedgerId,
      maxDedupSeconds: Int,
  )(implicit
      loggingContext: LoggingContext,
      materializer: Materializer,
  ) extends ReadServiceWithFeedSink {
    private val logger = ContextualizedLogger.get(getClass)
    private var stateUpdatesWasCalledAlready = false

    logger.info("Starting Sandbox-on-X read service...")

    val (feedSink, stateUpdatesSource) =
      MergeHub
        // We can't instrument these buffers, therefore keep these to minimal sizes and
        // use a configurable instrumented buffer in the producer.
        .source[(Offset, Update)](perProducerBufferSize = 1)
        .toMat(BroadcastHub.sink(bufferSize = 1))(Keep.both)
        .run()
        .tap { _ =>
          logger.info("Started Sandbox-on-X read service.")
        }

    override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
      Source.single(
        LedgerInitialConditions(
          ledgerId = ledgerId,
          config = Configuration(
            generation = 1L,
            timeModel = LedgerTimeModel.reasonableDefault,
            maxDeduplicationTime = java.time.Duration.ofSeconds(maxDedupSeconds.toLong),
          ),
          initialRecordTime = Timestamp.now(),
        )
      )

    override def stateUpdates(
        beginAfter: Option[Offset]
    )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] = {
      // TODO for PoC purposes:
      //   This method may only be called once, either with `beginAfter` set or unset.
      //   A second call will result in an error unless the server is restarted.
      //   Bootstrapping the bridge from indexer persistence is supported.
      synchronized {
        if (stateUpdatesWasCalledAlready)
          throw new IllegalStateException("not allowed to call this twice")
        else stateUpdatesWasCalledAlready = true
      }
      logger.info("Indexer subscribed to state updates.")
      beginAfter.foreach(offset =>
        logger.warn(
          s"Indexer subscribed from a specific offset $offset. This offset is not taking into consideration, and does not change the behavior of the ReadWriteServiceBridge. Only valid use case supported: service starting from an already ingested database, and indexer subscribes from exactly the ledger-end."
        )
      )

      stateUpdatesSource
    }

    override def currentHealth(): HealthStatus = Healthy
  }
}

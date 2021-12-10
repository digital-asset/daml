// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import ledger.api.health.{HealthStatus, Healthy}
import ledger.configuration.{Configuration, LedgerId, LedgerInitialConditions, LedgerTimeModel}
import ledger.offset.Offset
import ledger.participant.state.v2.{ReadService, Update}
import lf.data.Time.Timestamp
import logging.{ContextualizedLogger, LoggingContext}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.reactivestreams.Subscriber

trait ReadServiceWithFeedSubscriber extends ReadService {
  def subscriber: Subscriber[(Offset, Update)]
}

class SoXReadService(
    ledgerId: LedgerId,
    maxDedupSeconds: Int,
)(implicit
    loggingContext: LoggingContext,
    materializer: Materializer,
) extends ReadServiceWithFeedSubscriber {
  private val logger = ContextualizedLogger.get(getClass)
  private var stateUpdatesWasCalledAlready = false

  logger.info("Starting Sandbox-on-X read service...")

  // TODO do we need to onComplete this subscriber?
  val (subscriber: Subscriber[(Offset, Update)], stateUpdatesSource) =
    Source.asSubscriber[(Offset, Update)].preMaterialize()

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

  // TODO SoX: Implement health check (e.g. if state updates was called twice)
  override def currentHealth(): HealthStatus = Healthy
}

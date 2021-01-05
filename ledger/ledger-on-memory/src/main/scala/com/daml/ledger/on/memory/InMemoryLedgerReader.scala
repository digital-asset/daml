// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.participant.state.kvutils.OffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource

class InMemoryLedgerReader(
    override val ledgerId: LedgerId,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    metrics: Metrics)
    extends LedgerReader {
  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        startExclusive
          .map(OffsetBuilder.highestIndex(_).toInt)
          .getOrElse(StartIndex),
        RangeSource((startExclusive, endInclusive) =>
          Source.fromIterator(() => {
            Timed.value(
              metrics.daml.ledger.log.read,
              state
                .readLog(
                  _.view.zipWithIndex.map(_.swap).slice(startExclusive + 1, endInclusive + 1))
                .iterator)
          }))
      )
      .map { case (_, updates) => updates }

  override def currentHealth(): HealthStatus = Healthy
}

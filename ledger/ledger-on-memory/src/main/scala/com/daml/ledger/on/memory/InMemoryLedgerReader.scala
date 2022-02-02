// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.KVOffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource

class InMemoryLedgerReader(
    override val ledgerId: LedgerId,
    dispatcher: Dispatcher[Index],
    offsetBuilder: KVOffsetBuilder,
    state: InMemoryState,
    metrics: Metrics,
) extends LedgerReader {
  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        startExclusive
          .map(offsetBuilder.highestIndex(_).toInt)
          .getOrElse(StartIndex),
        RangeSource((startExclusive, endInclusive) =>
          Source.fromIterator(() => {
            Timed.value(
              metrics.daml.ledger.log.read,
              state
                .readLog(
                  _.view.zipWithIndex
                    .map(_.swap)
                    .slice(startExclusive + 1, endInclusive + 1)
                    .toVector // ensure we copy the results so we don't keep a reference to the log
                )
                .iterator,
            )
          })
        ),
      )
      .map { case (_, updates) => updates }

  override def currentHealth(): HealthStatus = Healthy
}

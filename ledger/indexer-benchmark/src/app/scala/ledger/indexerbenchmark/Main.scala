// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  LedgerReader,
  LedgerRecord,
}
import com.daml.ledger.participant.state.kvutils.export.ProtobufBasedLedgerDataImporter
import com.daml.ledger.participant.state.kvutils.{KVOffsetBuilder, Raw}
import com.daml.ledger.participant.state.v2.Update
import com.daml.metrics.Metrics

import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit =
    IndexerBenchmark.runAndExit(args, name => loadLedgerExport(name))

  private[this] def loadLedgerExport(config: Config): Future[Iterator[(Offset, Update)]] = {
    val path = Paths.get(config.updateSource)
    if (!Files.exists(path)) {
      throw new RuntimeException(s"Input file $path does not exist")
    }
    val importer = ProtobufBasedLedgerDataImporter(path)

    val offsetBuilder = new KVOffsetBuilder(0)
    val dataSource: Source[LedgerRecord, NotUsed] = Source
      .fromIterator(() => importer.read().iterator)
      .statefulMapConcat { () =>
        val nextOffset = new AtomicLong(0)

        { case (_, writeSet) =>
          writeSet.map { case (key, value) =>
            val offset = offsetBuilder.of(nextOffset.getAndIncrement())
            val logEntryId = Raw.LogEntryId(key.bytes) // `key` is of an unknown type.
            LedgerRecord(offset, logEntryId, value)
          }
        }
      }

    val keyValueSource = new LedgerReader {
      override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
        if (offset.isDefined) {
          Source.failed(
            new IllegalArgumentException(
              s"A read offset of $offset is not supported. Must be $None."
            )
          )
        } else {
          dataSource
        }

      override def currentHealth(): HealthStatus = Healthy

      override def ledgerId(): LedgerId = IndexerBenchmark.LedgerId
    }

    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    val keyValueStateReader = KeyValueParticipantStateReader(
      keyValueSource,
      metrics,
      failOnUnexpectedEvent = false,
    )

    // Note: this method is doing quite a lot of work to transform a sequence of write sets
    // to a sequence of state updates.
    // Note: this method eagerly loads the whole ledger export and transforms it into an array of state updates.
    // This will consume a lot of memory, but will avoid slowing down the indexer with write set decoding during
    // the benchmark.
    val system = ActorSystem("IndexerBenchmarkUpdateReader")
    implicit val materializer: Materializer = Materializer(system)
    keyValueStateReader
      .stateUpdates(None)
      .take(config.updateCount.getOrElse(Long.MaxValue))
      .zipWithIndex
      .map { case (data, index) =>
        if (index % 1000 == 0) println(s"Generated update $index")
        data
      }
      .runWith(Sink.seq[(Offset, Update)])
      .map(seq => seq.iterator)(DirectExecutionContext)
      .andThen { case _ => system.terminate() }(DirectExecutionContext)
  }
}

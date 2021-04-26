// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.{OffsetBuilder, Raw}
import com.daml.ledger.participant.state.kvutils.`export`.ProtobufBasedLedgerDataImporter
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  LedgerReader,
  LedgerRecord,
}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset, Update}
import com.daml.metrics.Metrics

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit =
    IndexerBenchmark.runAndExit(args, name => loadLedgerExport(name))

  private[this] def loadLedgerExport(name: String): Future[Iterator[(Offset, Update)]] = {
    val importer = ProtobufBasedLedgerDataImporter(Paths.get(name))
    val data = importer.read()

    val recordedBlocks = ListBuffer.empty[LedgerRecord]
    data.foreach { case (_, writeSet) =>
      writeSet.foreach { case (key, value) =>
        val offset = OffsetBuilder.fromLong(recordedBlocks.length.toLong)
        val logEntryId = Raw.LogEntryId(key.bytes) // `key` is of an unknown type.
        recordedBlocks.append(LedgerRecord(offset, logEntryId, value))
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
          Source.fromIterator(() => recordedBlocks.iterator)
        }

      override def currentHealth(): HealthStatus = HealthStatus.healthy

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
      .runWith(Sink.seq[(Offset, Update)])
      .map(_.toArray.iterator)(DirectExecutionContext)
  }
}

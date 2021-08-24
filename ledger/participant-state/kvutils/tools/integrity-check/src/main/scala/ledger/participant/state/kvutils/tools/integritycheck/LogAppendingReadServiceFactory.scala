// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.{LedgerId, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  LedgerReader,
  LedgerRecord,
}
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.participant.state.kvutils.{OffsetBuilder, Raw}
import com.daml.ledger.participant.state.v2.Update
import com.daml.metrics.Metrics

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

final class LogAppendingReadServiceFactory(
    metrics: Metrics
) extends ReplayingReadServiceFactory {
  private val recordedBlocks = ListBuffer.empty[LedgerRecord]

  override def appendBlock(writeSet: WriteSet): Unit =
    this.synchronized {
      writeSet.foreach { case (key, value) =>
        val offset = OffsetBuilder.fromLong(recordedBlocks.length.toLong)
        val logEntryId = Raw.LogEntryId(key.bytes) // `key` is of an unknown type.
        recordedBlocks.append(LedgerRecord(offset, logEntryId, value))
      }
    }

  /** Returns a new ReadService that can stream all previously recorded updates */
  override def createReadService(implicit materializer: Materializer): ReplayingReadService =
    this.synchronized {
      def recordedBlocksSnapshot: immutable.Seq[LedgerRecord] = recordedBlocks.toList

      val keyValueSource = new LedgerReader {
        override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
          if (offset.isDefined) {
            Source.failed(
              new IllegalArgumentException(
                s"A read offset of $offset is not supported. Must be $None."
              )
            )
          } else {
            Source.fromIterator(() => recordedBlocksSnapshot.iterator)
          }

        override def currentHealth(): HealthStatus = Healthy

        override def ledgerId(): LedgerId = "FakeParticipantStateReaderLedgerId"
      }

      val participantStateReader = {
        val implementation =
          KeyValueParticipantStateReader(
            keyValueSource,
            metrics,
            failOnUnexpectedEvent = false,
          )
        new ReplayingReadService {
          override def updateCount(): Long = recordedBlocksSnapshot.length.toLong

          override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
            implementation.ledgerInitialConditions()

          override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
            implementation.stateUpdates(beginAfter)

          override def currentHealth(): HealthStatus = implementation.currentHealth()
        }
      }

      participantStateReader
    }

  def expectedUpdateCount(): Long = recordedBlocks.length.toLong
}

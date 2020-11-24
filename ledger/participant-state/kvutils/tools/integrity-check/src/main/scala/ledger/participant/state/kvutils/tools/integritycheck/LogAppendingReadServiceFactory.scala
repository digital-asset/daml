// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.OffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  LedgerReader,
  LedgerRecord
}
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.participant.state.v1.{LedgerId, LedgerInitialConditions, Offset, Update}
import com.daml.metrics.Metrics

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

final class LogAppendingReadServiceFactory(
    metrics: Metrics,
) extends ReplayingReadServiceFactory {
  private val recordedBlocks = ListBuffer.empty[LedgerRecord]

  override def appendBlock(writeSet: WriteSet): Unit =
    this.synchronized {
      writeSet.foreach {
        case (key, value) =>
          val offset = OffsetBuilder.fromLong(recordedBlocks.length.toLong)
          recordedBlocks.append(LedgerRecord(offset, key, value))
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
                s"A read offset of $offset is not supported. Must be $None."))
          } else {
            Source.fromIterator(() => recordedBlocksSnapshot.toIterator)
          }

        override def currentHealth(): HealthStatus = HealthStatus.healthy

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

          override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
            implementation.getLedgerInitialConditions()

          override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
            implementation.stateUpdates(beginAfter)

          override def currentHealth(): HealthStatus = implementation.currentHealth()
        }
      }

      participantStateReader
    }

  def expectedUpdateCount(): Long = recordedBlocks.length.toLong
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.{Envelope, KVOffset, KeyValueConsumption}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Time
import com.daml.metrics.{Metrics, Timed}

class KeyValueParticipantStateReader(reader: LedgerReader, metrics: Metrics)(
    implicit materializer: Materializer)
    extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(createLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    Source
      .single(beginAfter.map(KVOffset.onlyKeepHighestIndex))
      .flatMapConcat(reader.events)
      .flatMapConcat {
        case LedgerRecord(offset, entryId, envelope) =>
          Timed
            .value(metrics.daml.kvutils.reader.openEnvelope, Envelope.open(envelope))
            .flatMap {
              case Envelope.LogEntryMessage(logEntry) =>
                Timed.value(
                  metrics.daml.kvutils.reader.parseUpdates, {
                    val logEntryId = DamlLogEntryId.parseFrom(entryId)
                    val updates = KeyValueConsumption.logEntryToUpdate(logEntryId, logEntry)
                    val updateOffset: (Offset, Int) => Offset =
                      if (updates.size > 1) KVOffset.setMiddleIndex else (offset, _) => offset
                    val updatesWithOffsets = Source(updates).zipWithIndex.map {
                      case (update, index) =>
                        updateOffset(offset, index.toInt) -> update
                    }
                    Right(updatesWithOffsets)
                  }
                )
              case _ =>
                Left("Envelope does not contain a log entry")
            }
            .getOrElse(throw new IllegalArgumentException(
              s"Invalid log entry received at offset $offset"))
      }
  }

  override def currentHealth(): HealthStatus =
    reader.currentHealth()

  private def createLedgerInitialConditions(): LedgerInitialConditions =
    LedgerInitialConditions(
      reader.ledgerId(),
      LedgerReader.DefaultConfiguration,
      Time.Timestamp.Epoch)
}

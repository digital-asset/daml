// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.{Envelope, KVOffset, KeyValueConsumption}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.ledger.api.health.HealthStatus

class KeyValueParticipantStateReader(reader: LedgerReader)(implicit materializer: Materializer)
    extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(createLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    Source
      .single(beginAfter.map(KVOffset.onlyKeepSignificantIndex))
      .flatMapConcat(reader.events)
      .flatMapConcat {
        case LedgerEntry.Heartbeat(offset, instant) =>
          val update = Update.Heartbeat(Time.Timestamp.assertFromInstant(instant))
          Source.single(offset -> update)
        case LedgerEntry.LedgerRecord(offset, entryId, envelope) =>
          Envelope
            .open(envelope)
            .flatMap {
              case Envelope.LogEntryMessage(logEntry) =>
                val updates = KeyValueConsumption.logEntryToUpdate(entryId, logEntry)
                val updateOffset: (Offset, Long) => Offset =
                  if (updates.size > 1) KVOffset.addSubIndex else (offset, _) => offset
                val updatesWithOffsets = Source(updates).zipWithIndex.map {
                  case (update, index) =>
                    updateOffset(offset, index) -> update
                }
                Right(updatesWithOffsets)
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

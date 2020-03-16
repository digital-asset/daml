// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueConsumption}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.ledger.api.health.HealthStatus

class KeyValueParticipantStateReader(reader: LedgerReader)(implicit materializer: Materializer)
    extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(createLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    reader
      .events(toReaderOffset(beginAfter))
      .flatMapConcat {
        case LedgerEntry.Heartbeat(offset, instant) =>
          val update = Update.Heartbeat(Time.Timestamp.assertFromInstant(instant))
          Source.single(toReturnedOffset(0, offset) -> update)
        case LedgerEntry.LedgerRecord(offset, entryId, envelope) =>
          Envelope
            .open(envelope)
            .flatMap {
              case Envelope.LogEntryMessage(logEntry) =>
                val logEntryId = DamlLogEntryId.parseFrom(entryId)
                val updates = Source(KeyValueConsumption.logEntryToUpdate(logEntryId, logEntry))
                val updatesWithOffsets = updates.zipWithIndex.map {
                  case (update, index) =>
                    toReturnedOffset(index, offset) -> update
                }
                Right(updatesWithOffsets)
              case _ =>
                Left("Envelope does not contain a log entry")
            }
            .getOrElse(throw new IllegalArgumentException(
              s"Invalid log entry received at offset $offset"))
      }
      .filter { case (offset, _) => beginAfter.forall(offset > _) }

  override def currentHealth(): HealthStatus =
    reader.currentHealth()

  private def toReaderOffset(offset: Option[Offset]): Option[Offset] =
    offset.collect {
      case beginAfter if beginAfter.components.size > 1 =>
        Offset(beginAfter.components.take(beginAfter.components.size - 1).toArray)
    }

  private def toReturnedOffset(index: Long, offset: Offset): Offset =
    Offset(Array.concat(offset.components.toArray, Array(index)))

  private def createLedgerInitialConditions(): LedgerInitialConditions =
    LedgerInitialConditions(
      reader.ledgerId(),
      LedgerReader.DefaultConfiguration,
      Time.Timestamp.Epoch)
}

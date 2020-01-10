// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueConsumption}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.ledger.api.health.HealthStatus

class KeyValueParticipantStateReader(reader: LedgerReader)(implicit materializer: Materializer)
    extends ReadService
    with AutoCloseable {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(createLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    reader
      .events(toReaderOffset(beginAfter))
      .flatMapConcat { record =>
        val updates = Envelope
          .open(record.envelope)
          .flatMap {
            case Envelope.LogEntryMessage(logEntry) =>
              Right[String, Seq[(Offset, Update)]](
                KeyValueConsumption
                  .logEntryToUpdate(record.entryId, logEntry)
                  .zipWithIndex
                  .map {
                    case (entry, index) =>
                      (toReturnedOffset(index, record.offset), entry)
                  })
            case _ => Left[String, Seq[(Offset, Update)]]("Envelope does not contain a log entry")
          }
          .getOrElse(throw new IllegalArgumentException(
            s"Invalid log entry received at offset ${record.offset}"))
        Source.fromIterator(() => updates.iterator)
      }
      .filter {
        case (offset, _) => beginAfter.forall(offset > _)
      }

  override def currentHealth(): HealthStatus = reader.currentHealth()

  override def close(): Unit = reader.close()

  private def toReaderOffset(offset: Option[Offset]): Option[Offset] =
    offset.collect {
      case beginAfter if beginAfter.components.size > 1 =>
        Offset(beginAfter.components.take(beginAfter.components.size - 1).toArray)
    }

  private def toReturnedOffset(index: Int, offset: Offset): Offset =
    Offset(Array.concat(offset.components.toArray, Array(index.toLong)))

  private def createLedgerInitialConditions(): LedgerInitialConditions =
    LedgerInitialConditions(
      reader.retrieveLedgerId(),
      LedgerReader.DefaultConfiguration,
      Time.Timestamp.Epoch)
}

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
    extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(createLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    reader
      .events(toReaderOffset(beginAfter))
      .flatMapConcat { record =>
        Envelope
          .open(record.envelope)
          .flatMap {
            case Envelope.LogEntryMessage(logEntry) =>
              val updates = Source(KeyValueConsumption.logEntryToUpdate(record.entryId, logEntry))
              val updatesWithOffsets = updates.zipWithIndex
                .map {
                  case (entry, index) =>
                    (toReturnedOffset(index, record.offset), entry)
                }
                .filter {
                  case (offset, _) => beginAfter.forall(offset > _)
                }
              Right(updatesWithOffsets)
            case _ =>
              Left("Envelope does not contain a log entry")
          }
          .getOrElse(throw new IllegalArgumentException(
            s"Invalid log entry received at offset ${record.offset}"))
      }

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

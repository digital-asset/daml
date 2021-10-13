// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.sql.Connection
import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.cache.{RawStringInterningCache, StringInterningCache}

import scala.util.chaining.scalaUtilChainingOps

trait SequentialWriteDao {
  def store(connection: Connection, offset: Offset, update: Option[state.Update]): Unit
}

case class SequentialWriteDaoImpl[DB_BATCH](
    storageBackend: IngestionStorageBackend[DB_BATCH] with ParameterStorageBackend,
    updateToDbDtos: Offset => state.Update => Iterator[DbDto],
    dbDtoToStringsForTemplateInterning: DbDto => Iterator[String],
    dbDtoToStringsForPartyInterning: DbDto => Iterator[String],
    stringInterningCache: StringInterningCache,
    ledgerEnd: AtomicReference[(Offset, Long)],
) extends SequentialWriteDao {

  private var lastEventSeqId: Long = _
  private var lastEventSeqIdInitialized = false

  private def lazyInit(): Unit =
    if (!lastEventSeqIdInitialized) {
      lastEventSeqId = ledgerEnd.get()._2
      lastEventSeqIdInitialized = true
    }

  private def nextEventSeqId: Long = {
    lastEventSeqId += 1
    lastEventSeqId
  }

  private def adaptEventSeqIds(dbDtos: Iterator[DbDto]): Vector[DbDto] =
    dbDtos.map {
      case e: DbDto.EventCreate => e.copy(event_sequential_id = nextEventSeqId)
      case e: DbDto.EventDivulgence => e.copy(event_sequential_id = nextEventSeqId)
      case e: DbDto.EventExercise => e.copy(event_sequential_id = nextEventSeqId)
      case notEvent => notEvent
    }.toVector

  override def store(connection: Connection, offset: Offset, update: Option[state.Update]): Unit =
    synchronized {
      lazyInit()

      val dbDtos = update
        .map(updateToDbDtos(offset))
        .map(adaptEventSeqIds)
        .getOrElse(Vector.empty)

      val newEntries = RawStringInterningCache.newEntries(
        strings = stringInterningCache.allRawEntries(
          templateEntries = dbDtos.iterator.flatMap(dbDtoToStringsForTemplateInterning),
          partyEntries = dbDtos.iterator.flatMap(dbDtoToStringsForPartyInterning),
        ),
        rawStringInterningCache = stringInterningCache.raw,
      )
      val dbDtosWithStringInterning =
        if (newEntries.isEmpty)
          dbDtos
        else {
          stringInterningCache.raw =
            RawStringInterningCache.from(newEntries, stringInterningCache.raw)
          dbDtos ++ newEntries.map { case (id, s) =>
            DbDto.StringInterningDto(id, s)
          }
        }

      dbDtosWithStringInterning
        .pipe(storageBackend.batch(_, stringInterningCache))
        .pipe(storageBackend.insertBatch(connection, _))

      storageBackend.updateLedgerEnd(
        ParameterStorageBackend.LedgerEnd(
          lastOffset = offset,
          lastEventSeqId = lastEventSeqId,
          lastStringInterningId = stringInterningCache.raw.lastId,
        )
      )(connection)

      ledgerEnd.set(offset -> lastEventSeqId)
    }
}

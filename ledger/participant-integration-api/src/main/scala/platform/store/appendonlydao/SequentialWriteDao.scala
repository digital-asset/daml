// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.sql.Connection

import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.platform.store.backend.{DBDTOV1, StorageBackend}

import scala.util.chaining.scalaUtilChainingOps

trait SequentialWriteDao {
  def store(connection: Connection, offset: Offset, update: Option[Update]): Unit
}

case class SequentialWriteDaoImpl[DB_BATCH](
    storageBackend: StorageBackend[DB_BATCH],
    updateToDbDtos: Offset => Update => Iterator[DBDTOV1],
) extends SequentialWriteDao {

  private var lastEventSeqId: Long = _
  private var lastEventSeqIdInitialized = false

  private def lazyInit(connection: Connection): Unit =
    if (!lastEventSeqIdInitialized) {
      lastEventSeqId = storageBackend.ledgerEnd(connection).lastEventSeqId.getOrElse(0)
      lastEventSeqIdInitialized = true
    }

  private def nextEventSeqId: Long = {
    lastEventSeqId += 1
    lastEventSeqId
  }

  private def adaptEventSeqIds(dbDtos: Iterator[DBDTOV1]): Vector[DBDTOV1] =
    dbDtos.map {
      case e: DBDTOV1.EventCreate => e.copy(event_sequential_id = nextEventSeqId)
      case e: DBDTOV1.EventDivulgence => e.copy(event_sequential_id = nextEventSeqId)
      case e: DBDTOV1.EventExercise => e.copy(event_sequential_id = nextEventSeqId)
      case notEvent => notEvent
    }.toVector

  override def store(connection: Connection, offset: Offset, update: Option[Update]): Unit =
    synchronized {
      lazyInit(connection)

      val dbDtos = update
        .map(updateToDbDtos(offset))
        .map(adaptEventSeqIds)
        .getOrElse(Vector.empty)

      dbDtos
        .pipe(storageBackend.batch)
        .pipe(storageBackend.insertBatch(connection, _))

      storageBackend.updateParams(
        connection,
        StorageBackend.Params(
          ledgerEnd = offset,
          eventSeqId = lastEventSeqId,
        ),
      )
    }
}

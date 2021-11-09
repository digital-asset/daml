// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.{
  DbDto,
  IngestionStorageBackend,
  ParameterStorageBackend,
  UpdateToDbDto,
}
import com.daml.platform.store.cache.MutableLedgerEndCache

import scala.concurrent.Future
import scala.util.chaining.scalaUtilChainingOps

trait SequentialWriteDao {
  def store(connection: Connection, offset: Offset, update: Option[state.Update]): Unit
}

object SequentialWriteDao {
  def apply(
      participantId: Ref.ParticipantId,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      metrics: Metrics,
      compressionStrategy: CompressionStrategy,
      ledgerEndCache: MutableLedgerEndCache,
      ingestionStorageBackend: IngestionStorageBackend[_],
      parameterStorageBackend: ParameterStorageBackend,
  ): SequentialWriteDao =
    SequentialWriteDaoImpl(
      ingestionStorageBackend = ingestionStorageBackend,
      parameterStorageBackend = parameterStorageBackend,
      updateToDbDtos = UpdateToDbDto(
        participantId = participantId,
        translation = new LfValueTranslation(
          cache = lfValueTranslationCache,
          metrics = metrics,
          enricherO = None,
          loadPackage = (_, _) => Future.successful(None),
        ),
        compressionStrategy = compressionStrategy,
      ),
      ledgerEndCache = ledgerEndCache,
    )

  val noop: SequentialWriteDao = NoopSequentialWriteDao
}

private[appendonlydao] object NoopSequentialWriteDao extends SequentialWriteDao {
  override def store(connection: Connection, offset: Offset, update: Option[Update]): Unit =
    throw new UnsupportedOperationException
}

private[appendonlydao] case class SequentialWriteDaoImpl[DB_BATCH](
    ingestionStorageBackend: IngestionStorageBackend[DB_BATCH],
    parameterStorageBackend: ParameterStorageBackend,
    updateToDbDtos: Offset => state.Update => Iterator[DbDto],
    ledgerEndCache: MutableLedgerEndCache,
) extends SequentialWriteDao {

  private var lastEventSeqId: Long = _
  private var lastEventSeqIdInitialized = false

  private def lazyInit(connection: Connection): Unit =
    if (!lastEventSeqIdInitialized) {
      lastEventSeqId = parameterStorageBackend.ledgerEndOrBeforeBegin(connection).lastEventSeqId
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
      lazyInit(connection)

      val dbDtos = update
        .map(updateToDbDtos(offset))
        .map(adaptEventSeqIds)
        .getOrElse(Vector.empty)

      dbDtos
        .pipe(ingestionStorageBackend.batch)
        .pipe(ingestionStorageBackend.insertBatch(connection, _))

      parameterStorageBackend.updateLedgerEnd(
        ParameterStorageBackend.LedgerEnd(
          lastOffset = offset,
          lastEventSeqId = lastEventSeqId,
        )
      )(connection)

      ledgerEndCache.set(offset -> lastEventSeqId)
    }
}

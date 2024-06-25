// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.daml.lf.data.Ref
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.{
  DbDto,
  DbDtoToStringsForInterning,
  IngestionStorageBackend,
  ParameterStorageBackend,
  UpdateToDbDto,
}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.digitalasset.canton.platform.store.interning.{
  DomainStringIterators,
  InternizingStringInterningView,
  StringInterning,
}
import com.digitalasset.canton.tracing.Traced

import java.sql.Connection
import scala.concurrent.{Future, blocking}
import scala.util.chaining.scalaUtilChainingOps

trait SequentialWriteDao {
  def store(connection: Connection, offset: Offset, update: Option[Traced[Update]]): Unit
}

object SequentialWriteDao {
  def apply(
      participantId: Ref.ParticipantId,
      metrics: LedgerApiServerMetrics,
      compressionStrategy: CompressionStrategy,
      ledgerEndCache: MutableLedgerEndCache,
      stringInterningView: StringInterning with InternizingStringInterningView,
      ingestionStorageBackend: IngestionStorageBackend[_],
      parameterStorageBackend: ParameterStorageBackend,
      loggerFactory: NamedLoggerFactory,
  ): SequentialWriteDao = {
    MetricsContext.withMetricLabels("participant_id" -> participantId) { implicit mc =>
      SequentialWriteDaoImpl(
        ingestionStorageBackend = ingestionStorageBackend,
        parameterStorageBackend = parameterStorageBackend,
        updateToDbDtos = UpdateToDbDto(
          participantId = participantId,
          translation = new LfValueTranslation(
            metrics = metrics,
            engineO = None,
            loadPackage = (_, _) => Future.successful(None),
            loggerFactory = loggerFactory,
          ),
          compressionStrategy = compressionStrategy,
          metrics,
        ),
        ledgerEndCache = ledgerEndCache,
        stringInterningView = stringInterningView,
        dbDtosToStringsForInterning = DbDtoToStringsForInterning(_),
      )
    }
  }

  val noop: SequentialWriteDao = NoopSequentialWriteDao
}

private[dao] object NoopSequentialWriteDao extends SequentialWriteDao {
  override def store(connection: Connection, offset: Offset, update: Option[Traced[Update]]): Unit =
    throw new UnsupportedOperationException
}

private[dao] final case class SequentialWriteDaoImpl[DB_BATCH](
    ingestionStorageBackend: IngestionStorageBackend[DB_BATCH],
    parameterStorageBackend: ParameterStorageBackend,
    updateToDbDtos: Offset => Traced[Update] => Iterator[DbDto],
    ledgerEndCache: MutableLedgerEndCache,
    stringInterningView: StringInterning with InternizingStringInterningView,
    dbDtosToStringsForInterning: Iterable[DbDto] => DomainStringIterators,
) extends SequentialWriteDao {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lastEventSeqId: Long = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lastStringInterningId: Int = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lastEventSeqIdInitialized = false
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var previousTransactionMetaToEventSeqId: Long = _

  private def lazyInit(connection: Connection): Unit =
    if (!lastEventSeqIdInitialized) {
      val ledgerEnd = parameterStorageBackend.ledgerEnd(connection)
      lastEventSeqId = ledgerEnd.lastEventSeqId
      previousTransactionMetaToEventSeqId = ledgerEnd.lastEventSeqId
      lastStringInterningId = ledgerEnd.lastStringInterningId
      lastEventSeqIdInitialized = true
    }

  private def nextEventSeqId: Long = {
    lastEventSeqId += 1
    lastEventSeqId
  }

  private def adaptEventSeqIds(dbDtos: Iterator[DbDto]): Vector[DbDto] =
    dbDtos.map {
      case e: DbDto.EventCreate => e.copy(event_sequential_id = nextEventSeqId)
      case e: DbDto.EventExercise => e.copy(event_sequential_id = nextEventSeqId)
      case e: DbDto.IdFilterCreateStakeholder =>
        e.copy(event_sequential_id = lastEventSeqId)
      case e: DbDto.IdFilterCreateNonStakeholderInformee =>
        e.copy(event_sequential_id = lastEventSeqId)
      case e: DbDto.IdFilterConsumingStakeholder =>
        e.copy(event_sequential_id = lastEventSeqId)
      case e: DbDto.IdFilterConsumingNonStakeholderInformee =>
        e.copy(event_sequential_id = lastEventSeqId)
      case e: DbDto.IdFilterNonConsumingInformee =>
        e.copy(event_sequential_id = lastEventSeqId)
      case e: DbDto.TransactionMeta =>
        val dto = e.copy(
          event_sequential_id_first = (previousTransactionMetaToEventSeqId + 1),
          event_sequential_id_last = lastEventSeqId,
        )
        previousTransactionMetaToEventSeqId = lastEventSeqId
        dto
      case notEvent => notEvent
    }.toVector

  override def store(connection: Connection, offset: Offset, update: Option[Traced[Update]]): Unit =
    blocking(synchronized {
      lazyInit(connection)

      val dbDtos = update
        .map(updateToDbDtos(offset))
        .map(adaptEventSeqIds)
        .getOrElse(Vector.empty)

      val dbDtosWithStringInterning =
        dbDtos
          .pipe(dbDtosToStringsForInterning)
          .pipe(stringInterningView.internize)
          .map(DbDto.StringInterningDto.from)
          .pipe(newEntries =>
            newEntries.lastOption
              .fold(dbDtos)(last => {
                lastStringInterningId = last.internalId
                dbDtos ++ newEntries
              })
          )

      dbDtosWithStringInterning
        .pipe(ingestionStorageBackend.batch(_, stringInterningView))
        .pipe(ingestionStorageBackend.insertBatch(connection, _))

      parameterStorageBackend.updateLedgerEnd(
        ParameterStorageBackend.LedgerEnd(
          lastOffset = offset,
          lastEventSeqId = lastEventSeqId,
          lastStringInterningId = lastStringInterningId,
        )
      )(connection)

      ledgerEndCache.set(offset -> lastEventSeqId)
    })
}

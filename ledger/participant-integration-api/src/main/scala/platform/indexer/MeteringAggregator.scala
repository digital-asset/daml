// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.ParticipantMetering
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.indexer.MeteringAggregator.{toOffsetDateTime, toTimestamp}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.daml.platform.store.backend.{
  MeteringParameterStorageBackend,
  MeteringStorageWriteBackend,
  ParameterStorageBackend,
}

import java.sql.Connection
import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.{Timer, TimerTask}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object MeteringAggregator {

  private val logger = ContextualizedLogger.get(getClass)

  class Owner(
      meteringStore: MeteringStorageWriteBackend,
      parameterStore: ParameterStorageBackend,
      meteringParameterStore: MeteringParameterStorageBackend,
      metrics: Metrics,
      period: FiniteDuration = 6.minutes,
      maxTaskDuration: FiniteDuration = 6.hours,
  ) {

    private[platform] def apply(
        dbDispatcher: DbDispatcher
    )(implicit loggingContext: LoggingContext): ResourceOwner[Unit] = {
      val aggregator = new MeteringAggregator(
        meteringStore,
        parameterStore,
        meteringParameterStore,
        metrics,
        dbDispatcher,
      )
      for {
        _ <- ResourceOwner.forFuture(() => aggregator.initialize())
        _ <- ResourceOwner.forTimer(() => new Timer()).map { timer =>
          timer.scheduleAtFixedRate(
            new TimerTask {
              override def run(): Unit = {
                Try {
                  Await.ready(aggregator.run(), maxTaskDuration)
                } match {
                  case Success(_) => ()
                  case Failure(e) =>
                    logger.error(s"Metering not aggregated after $maxTaskDuration", e)
                }
              }
            },
            period.toMillis,
            period.toMillis,
          )
        }
      } yield ()
    }
  }

  private def toTimestamp(dateTime: OffsetDateTime): Timestamp =
    Timestamp.assertFromInstant(dateTime.toInstant)
  private def toOffsetDateTime(timestamp: Timestamp): OffsetDateTime =
    OffsetDateTime.ofInstant(timestamp.toInstant, ZoneOffset.UTC)

}

class MeteringAggregator(
    meteringStore: MeteringStorageWriteBackend,
    parameterStore: ParameterStorageBackend,
    meteringParameterStore: MeteringParameterStorageBackend,
    metrics: Metrics,
    dbDispatcher: DbDispatcher,
    clock: () => Timestamp = () => Timestamp.now(),
)(implicit loggingContext: LoggingContext) {

  private val parasitic: ExecutionContext = ExecutionContext.parasitic

  private val logger = ContextualizedLogger.get(getClass)

  private[platform] def initialize(): Future[Unit] = {
    val initTimestamp = toOffsetDateTime(clock()).truncatedTo(ChronoUnit.HOURS).minusHours(1)
    val initLedgerMeteringEnd = LedgerMeteringEnd(Offset.beforeBegin, toTimestamp(initTimestamp))
    dbDispatcher.executeSql(metrics.daml.index.db.initializeMeteringAggregator) {
      meteringParameterStore.initializeLedgerMeteringEnd(initLedgerMeteringEnd)
    }
  }

  private[platform] def run(): Future[Unit] = {

    val future = dbDispatcher.executeSql(metrics.daml.index.db.meteringAggregator) { conn =>
      val nowUtcTime = toOffsetDateTime(clock())
      val lastLedgerMeteringEnd = meteringParameterStore.assertLedgerMeteringEnd(conn)
      val startUtcTime: OffsetDateTime = toOffsetDateTime(lastLedgerMeteringEnd.timestamp)
      val endUtcTime = startUtcTime.plusHours(1)

      if (nowUtcTime.isAfter(endUtcTime)) {

        val toEndTime = toTimestamp(endUtcTime)
        val ingestedLedgerEnd = parameterStore.ledgerEnd(conn).lastOffset
        val maybeMaxOffset =
          meteringStore.transactionMeteringMaxOffset(lastLedgerMeteringEnd.offset, toEndTime)(conn)

        val (
          periodIngested, // This is true if the time period is closed fully ingested
          hasMetering, // This is true if there are transaction_metering records to aggregate
          toOffsetEnd, // This is the 'to' offset for the period being aggregated
        ) = maybeMaxOffset match {
          case Some(offset) => (offset <= ingestedLedgerEnd, true, offset)
          case None => (true, false, lastLedgerMeteringEnd.offset)
        }

        if (periodIngested) {
          Some(
            aggregate(
              conn = conn,
              lastLedgerMeteringEnd = lastLedgerMeteringEnd,
              thisLedgerMeteringEnd = LedgerMeteringEnd(toOffsetEnd, toEndTime),
              hasMetering = hasMetering,
            )
          )
        } else {
          logger.info("Not all transaction metering for aggregation time period is yet ingested")
          None
        }
      } else {
        None
      }
    }

    future.onComplete({
      case Success(None) => logger.debug("No transaction metering aggregation required")
      case Success(Some(lme)) =>
        logger.info(s"Aggregating transaction metering completed up to $lme")
      case Failure(e) => logger.error("Failed to aggregate transaction metering", e)
    })(parasitic)

    future.map(_ => ())(parasitic)
  }

  private def aggregate(
      conn: Connection,
      lastLedgerMeteringEnd: LedgerMeteringEnd,
      thisLedgerMeteringEnd: LedgerMeteringEnd,
      hasMetering: Boolean,
  ): LedgerMeteringEnd = {
    logger.info(s"Aggregating transaction metering for $thisLedgerMeteringEnd")

    if (hasMetering) {
      populateParticipantMetering(conn, lastLedgerMeteringEnd, thisLedgerMeteringEnd)
    }

    meteringParameterStore.updateLedgerMeteringEnd(thisLedgerMeteringEnd)(conn)

    thisLedgerMeteringEnd
  }

  private def populateParticipantMetering(
      conn: Connection,
      lastLedgerMeteringEnd: LedgerMeteringEnd,
      thisLedgerMeteringEnd: LedgerMeteringEnd,
  ): Unit = {

    val applicationCounts =
      meteringStore.selectTransactionMetering(
        lastLedgerMeteringEnd.offset,
        thisLedgerMeteringEnd.offset,
      )(
        conn
      )

    val participantMetering = applicationCounts.map { case (applicationId, actionCount) =>
      ParticipantMetering(
        applicationId = applicationId,
        from = lastLedgerMeteringEnd.timestamp,
        to = thisLedgerMeteringEnd.timestamp,
        actionCount = actionCount,
        ledgerOffset = thisLedgerMeteringEnd.offset,
      )
    }.toVector

    meteringStore.insertParticipantMetering(participantMetering)(conn)

    meteringStore.deleteTransactionMetering(
      lastLedgerMeteringEnd.offset,
      thisLedgerMeteringEnd.offset,
    )(
      conn
    )

  }
}

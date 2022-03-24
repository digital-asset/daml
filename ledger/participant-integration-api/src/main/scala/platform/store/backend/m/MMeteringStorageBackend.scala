// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref.{ApplicationId, HexString}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.{
  DbDto,
  MeteringParameterStorageBackend,
  MeteringStorageReadBackend,
  MeteringStorageWriteBackend,
}

object MMeteringParameterStorageBackend extends MeteringParameterStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def initializeLedgerMeteringEnd(
      init: MeteringParameterStorageBackend.LedgerMeteringEnd
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit =
    MStore.update(connection) { mStore =>
      ledgerMeteringEnd(connection) match {
        case None =>
          logger.info(s"Initializing ledger metering end to $init")
          mStore.copy(meteringLedgerEnd = init)

        case Some(existing) =>
          logger.info(s"Found existing ledger metering end $existing")
          mStore
      }
    }

  override def ledgerMeteringEnd(
      connection: Connection
  ): Option[MeteringParameterStorageBackend.LedgerMeteringEnd] =
    Option(MStore(connection)(_.meteringLedgerEnd))

  override def assertLedgerMeteringEnd(
      connection: Connection
  ): MeteringParameterStorageBackend.LedgerMeteringEnd =
    ledgerMeteringEnd(connection).getOrElse(
      throw new IllegalStateException("Ledger metering is not initialized")
    )

  override def updateLedgerMeteringEnd(
      ledgerMeteringEnd: MeteringParameterStorageBackend.LedgerMeteringEnd
  )(connection: Connection): Unit =
    MStore.update(connection)(_.copy(meteringLedgerEnd = ledgerMeteringEnd))
}

object MMeteringStorageReadBackend extends MeteringStorageReadBackend {

  // TODO this is a complete copy paste. Can it be that this should be higher level, and only the two private methods should belong in StorageBackend?
  override def reportData(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      maybeApplicationId: Option[ApplicationId],
  )(connection: Connection): MeteringStore.ReportData = {
    val ledgerMeteringEnd = MMeteringParameterStorageBackend.assertLedgerMeteringEnd(connection)
    val participantData = participantMetering(from, to, maybeApplicationId)(connection)
    val isFinal = to.fold(false)(ledgerMeteringEnd.timestamp >= _)
    val data = if (isFinal) {
      participantData
    } else {
      val transactionData =
        transactionMetering(ledgerMeteringEnd.offset, to, maybeApplicationId)(connection)
      val apps: Set[ApplicationId] = participantData.keySet ++ transactionData.keySet
      apps.toList.map { a =>
        a -> (participantData.getOrElse(a, 0L) + transactionData.getOrElse(a, 0L))
      }.toMap
    }

    ReportData(data, isFinal)
  }

  private def transactionMetering(
      from: Offset,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = MStore(connection) { mStore =>
    val hexOffset = from.toHexString.toString
    mStore.transactionMetering.iterator
      .dropWhile(_.ledger_offset <= hexOffset)
      .takeWhile {
        case _ if to.isEmpty => true
        case transactionMetering => transactionMetering.metering_timestamp < to.get.micros
      }
      .filter(transactionMetering => appId.forall(_ == transactionMetering.application_id))
      .foldLeft(Map.empty[String, Long]) { case (acc, transactionMetering) =>
        acc + (transactionMetering.application_id -> (acc.getOrElse(
          transactionMetering.application_id,
          0L,
        ) + transactionMetering.action_count))
      }
      .map { case (appId, count) =>
        Ref.ApplicationId.assertFromString(appId) -> count
      }
  }

  private def participantMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = MStore(connection) { mStore =>
    mStore.participantMetering.iterator
      .dropWhile(_.from < from)
      .takeWhile {
        case _ if to.isEmpty => true
        case participantMetering => participantMetering.to <= to.get
      }
      .filter(transactionMetering => appId.forall(_ == transactionMetering.applicationId.toString))
      .foldLeft(Map.empty[ApplicationId, Long]) { case (acc, participantMetering) =>
        acc + (participantMetering.applicationId -> (acc.getOrElse(
          participantMetering.applicationId,
          0L,
        ) + participantMetering.actionCount))
      }
  }
}

object MMeteringStorageWriteBackend extends MeteringStorageWriteBackend {
  override def transactionMeteringMaxOffset(from: Offset, to: Time.Timestamp)(
      connection: Connection
  ): Option[Offset] = MStore(connection) { mStore =>
    val fromHexOffset = from.toHexString.toString
    mStore.transactionMetering.iterator
      .dropWhile(_.ledger_offset <= fromHexOffset)
      .takeWhile(_.metering_timestamp < to.micros)
      .foldLeft(Option.empty[DbDto.TransactionMetering])((_, next) => Some(next))
      .map(_.ledger_offset)
      .map(HexString.assertFromString)
      .map(Offset.fromHexString)
  }

  override def selectTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Map[ApplicationId, Int] = MStore(connection) { mStore =>
    val hexFromOffset = from.toHexString.toString
    val hexToOffset = to.toHexString.toString
    mStore.transactionMetering.iterator
      .dropWhile(_.ledger_offset <= hexFromOffset)
      .takeWhile(_.ledger_offset <= hexToOffset)
      .foldLeft(Map.empty[String, Int]) { case (acc, transactionMetering) =>
        acc + (transactionMetering.application_id -> (acc.getOrElse(
          transactionMetering.application_id,
          0,
        ) + 1))
      }
      .map { case (appId, count) =>
        Ref.ApplicationId.assertFromString(appId) -> count
      }
  }

  override def deleteTransactionMetering(from: Offset, to: Offset)(connection: Connection): Unit =
    MStore.update(connection) { mStore =>
      val hexFromOffset = from.toHexString.toString
      val hexToOffset = to.toHexString.toString
      mStore.copy(
        transactionMetering = mStore.transactionMetering.iterator
          .filterNot(transactionMetering =>
            transactionMetering.ledger_offset > hexFromOffset &&
              transactionMetering.ledger_offset <= hexToOffset
          )
          .toVector
      )
    }

  override def insertParticipantMetering(
      metering: Vector[MeteringStore.ParticipantMetering]
  )(connection: Connection): Unit = MStore.update(connection) { mStore =>
    mStore.copy(
      participantMetering = mStore.participantMetering ++ metering
    )
  }

  override def allParticipantMetering()(
      connection: Connection
  ): Vector[MeteringStore.ParticipantMetering] =
    MStore(connection)(_.participantMetering)
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.executors.executors.QueueAwareExecutionContextExecutorService
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  TransactionMetering,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{
  MeteringParameterStorageBackend,
  MeteringStorageWriteBackend,
  ParameterStorageBackend,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.mockito.MockitoSugar
import org.mockito.captor.ArgCaptor
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.sql.Connection
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

//noinspection TypeAnnotation
final class MeteringAggregatorSpec
    extends AnyWordSpecLike
    with MockitoSugar
    with Matchers
    with TestEssentials {

  private val metrics = LedgerApiServerMetrics.ForTesting
  private def toTS(t: OffsetDateTime): Timestamp = Timestamp.assertFromInstant(t.toInstant)

  "MeteringAggregator" should {

    val applicationA = Ref.ApplicationId.assertFromString("appA")
    val applicationB = Ref.ApplicationId.assertFromString("appB")

    class TestSetup {

      val lastAggEndTime: OffsetDateTime =
        OffsetDateTime.of(LocalDate.now(), LocalTime.of(15, 0), ZoneOffset.UTC)
      val nextAggEndTime: OffsetDateTime = lastAggEndTime.plusHours(1)
      val timeNow: OffsetDateTime = lastAggEndTime.plusHours(1).plusMinutes(+5)
      val lastAggOffset: Offset = Offset.fromHexString(Ref.HexString.assertFromString("01"))

      val conn: Connection = mock[Connection]
      val dispatcher: DbDispatcher = new DbDispatcher {
        override def executeSql[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[T] = Future.successful {
          sql(conn)
        }
        override val executor: QueueAwareExecutionContextExecutorService =
          mock[QueueAwareExecutionContextExecutorService]
      }

      val parameterStore: ParameterStorageBackend = mock[ParameterStorageBackend]
      val meteringParameterStore: MeteringParameterStorageBackend =
        mock[MeteringParameterStorageBackend]
      val meteringStore: MeteringStorageWriteBackend = mock[MeteringStorageWriteBackend]

      def runUnderTest(
          transactionMetering: Vector[TransactionMetering],
          maybeLedgerEnd: Option[Offset] = None,
      ): Future[Unit] = {

        val applicationCounts = transactionMetering
          .groupMapReduce(_.applicationId)(_.actionCount)(_ + _)

        val ledgerEndOffset = (maybeLedgerEnd, transactionMetering.lastOption) match {
          case (Some(le), _) => le
          case (None, Some(t)) => t.ledgerOffset
          case (None, None) => lastAggOffset
        }

        when(meteringParameterStore.assertLedgerMeteringEnd(conn))
          .thenReturn(LedgerMeteringEnd(lastAggOffset, toTS(lastAggEndTime)))

        when(meteringStore.transactionMeteringMaxOffset(lastAggOffset, toTS(nextAggEndTime))(conn))
          .thenReturn(transactionMetering.lastOption.map(_.ledgerOffset))

        when(parameterStore.ledgerEnd(conn))
          .thenReturn(LedgerEnd(ledgerEndOffset, 0L, 0))

        transactionMetering.lastOption.map { last =>
          when(meteringStore.selectTransactionMetering(lastAggOffset, last.ledgerOffset)(conn))
            .thenReturn(applicationCounts)
        }

        new MeteringAggregator(
          meteringStore,
          parameterStore,
          meteringParameterStore,
          metrics,
          dispatcher,
          () => toTS(timeNow),
          loggerFactory = loggerFactory,
        )
          .run()

      }
    }

    "aggregate transaction metering records" in new TestSetup {

      val transactionMetering = Vector(10, 15, 20).map { i =>
        TransactionMetering(
          applicationId = applicationA,
          actionCount = i,
          meteringTimestamp = toTS(lastAggEndTime.plusMinutes(i.toLong)),
          ledgerOffset = Offset.fromHexString(Ref.HexString.assertFromString(i.toString)),
        )
      }

      val expected: ParticipantMetering = ParticipantMetering(
        applicationA,
        from = toTS(lastAggEndTime),
        to = toTS(nextAggEndTime),
        actionCount = transactionMetering.map(_.actionCount).sum,
        transactionMetering.last.ledgerOffset,
      )

      runUnderTest(transactionMetering).discard

      verify(meteringStore).insertParticipantMetering(Vector(expected))(conn)
      verify(meteringParameterStore).updateLedgerMeteringEnd(
        LedgerMeteringEnd(expected.ledgerOffset, expected.to)
      )(conn)
      verify(meteringStore).deleteTransactionMetering(
        lastAggOffset,
        transactionMetering.last.ledgerOffset,
      )(conn)

    }

    "not aggregate if there is not a full period" in new TestSetup {
      override val timeNow = lastAggEndTime.plusHours(1).plusMinutes(-5)
      when(meteringParameterStore.assertLedgerMeteringEnd(conn))
        .thenReturn(LedgerMeteringEnd(lastAggOffset, toTS(lastAggEndTime)))
      runUnderTest(Vector.empty).discard
      verifyNoMoreInteractions(meteringStore)
    }

    "aggregate over multiple applications" in new TestSetup {

      val expected = Set(applicationA, applicationB)

      val transactionMetering = expected.toVector.map { a =>
        TransactionMetering(
          applicationId = a,
          actionCount = 1,
          meteringTimestamp = toTS(lastAggEndTime.plusMinutes(1)),
          ledgerOffset = Offset.fromHexString(Ref.HexString.assertFromString("10")),
        )
      }

      runUnderTest(transactionMetering).discard

      val participantMeteringCaptor = ArgCaptor[Vector[ParticipantMetering]]
      verify(meteringStore).insertParticipantMetering(participantMeteringCaptor)(any[Connection])
      participantMeteringCaptor.value.map(_.applicationId).toSet shouldBe expected

    }

    "increase ledger metering end even if there are not transaction metering records" in new TestSetup {

      runUnderTest(Vector.empty[TransactionMetering]).discard

      verify(meteringParameterStore).updateLedgerMeteringEnd(
        LedgerMeteringEnd(lastAggOffset, toTS(lastAggEndTime.plusHours(1)))
      )(conn)

    }

    "skip aggregation if the last transaction metering offset within the time range has not been fully ingested" in new TestSetup {

      val transactionMetering = Vector(
        TransactionMetering(
          applicationId = applicationA,
          actionCount = 1,
          meteringTimestamp = toTS(lastAggEndTime.plusMinutes(1)),
          ledgerOffset = Offset.fromHexString(Ref.HexString.assertFromString("03")),
        )
      )

      runUnderTest(
        transactionMetering,
        maybeLedgerEnd = Some(Offset.fromHexString(Ref.HexString.assertFromString("02"))),
      ).discard

      verify(meteringParameterStore, never).updateLedgerMeteringEnd(any[LedgerMeteringEnd])(
        any[Connection]
      )

    }

    "fail if an attempt is made to run un-initialized" in new TestSetup {
      // Note this only works as we do not use a real future for testing
      intercept[IllegalStateException] {
        when(meteringParameterStore.assertLedgerMeteringEnd(conn))
          .thenThrow(new IllegalStateException("Blah"))
        val underTest =
          new MeteringAggregator(
            meteringStore,
            parameterStore,
            meteringParameterStore,
            metrics,
            dispatcher,
            () => toTS(timeNow),
            loggerFactory = loggerFactory,
          )
        underTest.run()
      }
    }

    "initialize the metering ledger end to the hour before the current hour" in new TestSetup {
      val underTest =
        new MeteringAggregator(
          meteringStore,
          parameterStore,
          meteringParameterStore,
          metrics,
          dispatcher,
          () => toTS(timeNow),
          loggerFactory = loggerFactory,
        )
      underTest.initialize().discard
      val expected = LedgerMeteringEnd(
        Offset.beforeBegin,
        toTS(timeNow.truncatedTo(ChronoUnit.HOURS).minusHours(1)),
      )
      verify(meteringParameterStore).initializeLedgerMeteringEnd(expected, loggerFactory)(conn)
    }

  }

}

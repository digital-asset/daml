// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  ReportData,
  TransactionMetering,
}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside}

private[backend] trait StorageBackendTestsReadMetering
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  {
    behavior of "StorageBackend (metering report data)"

    val userIdA: Ref.UserId = Ref.UserId.assertFromString("appA")
    val userIdB: Ref.UserId = Ref.UserId.assertFromString("appB")

    def buildTransactionMetering(
        index: Long,
        userId: Ref.UserId,
        aggregated: Boolean = false,
    ) = TransactionMetering(
      userId = userId,
      actionCount = if (aggregated) 1000 else index.toInt, // Should not be used in tests below
      meteringTimestamp = someTime.addMicros(index),
      ledgerOffset = offset(index),
    )

    def buildParticipantMetering(index: Long, userId: Ref.UserId) = ParticipantMetering(
      userId = userId,
      from = someTime.addMicros(index - 1),
      to = someTime.addMicros(index),
      actionCount = index.toInt,
      ledgerOffset = Some(offset(index)),
    )

    val participantMetering = Vector(
      buildParticipantMetering(1, userIdA),
      buildParticipantMetering(2, userIdA),
      buildParticipantMetering(3, userIdB),
      buildParticipantMetering(4, userIdA),
      buildParticipantMetering(5, userIdA),
    )
    discard(participantMetering)
    val ledgerMeteringEnd =
      LedgerMeteringEnd(Some(offset(5L)), someTime.addMicros(6L))
    val loggerFactory = SuppressingLogger(getClass)

    // Aggregated transaction metering should never be read
    val aggregatedTransactionMetering = Vector(
      buildTransactionMetering(1, userIdA, aggregated = true),
      buildTransactionMetering(2, userIdA, aggregated = true),
      buildTransactionMetering(3, userIdB, aggregated = true),
      buildTransactionMetering(4, userIdA, aggregated = true),
      buildTransactionMetering(5, userIdA, aggregated = true),
    )

    val transactionMetering = Vector(
      buildTransactionMetering(11, userIdA),
      buildTransactionMetering(12, userIdA),
      buildTransactionMetering(13, userIdB),
      buildTransactionMetering(14, userIdA),
      buildTransactionMetering(15, userIdA),
    )

    def populate(): Unit = {
      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(
        ingest(
          (aggregatedTransactionMetering ++ transactionMetering).map(dtoTransactionMetering),
          _,
        )
      )
      executeSql(backend.metering.write.insertParticipantMetering(participantMetering))
      executeSql(
        backend.meteringParameter.initializeLedgerMeteringEnd(ledgerMeteringEnd, loggerFactory)
      )
    }

    def execute(
        from: Timestamp,
        to: Option[Timestamp],
        userId: Option[UserId],
    ): ReportData = {

      val participantMap =
        participantMetering
          .filter(_.from >= from)
          .filter(m => to.fold(true)(m.to <= _))
          .filter(m => userId.fold(true)(m.userId == _))
          .groupMapReduce(_.userId)(_.actionCount.toLong)(_ + _)

      val transactionMap =
        transactionMetering
          .filter(tm => Option(tm.ledgerOffset) > ledgerMeteringEnd.offset)
          .filter(m => to.fold(true)(m.meteringTimestamp < _))
          .filter(m => userId.fold(true)(m.userId == _))
          .groupMapReduce(_.userId)(_.actionCount.toLong)(_ + _)

      val apps: Set[UserId] = participantMap.keySet ++ transactionMap.keySet

      val metering = apps.toList.map { a =>
        a -> (participantMap.getOrElse(a, 0L) + transactionMap.getOrElse(a, 0L))
      }.toMap

      val isFinal = to.fold(false)(ledgerMeteringEnd.timestamp >= _)

      ReportData(metering, isFinal)

    }

    def check(
        fromIdx: Long,
        toIdx: Option[Long],
        userId: Option[UserId],
    ): Assertion = {
      populate()
      val from = someTime.addMicros(fromIdx)
      val to = toIdx.map(someTime.addMicros)
      val actual = executeSql(
        backend.metering.read.reportData(from, to, userId)
      )
      val expected = execute(from, to, userId)
      actual shouldBe expected
    }

    it should "create a final report if the aggregation timestamp exceeds the requested to-timestamp" in {
      check(2, toIdx = Some(4), None)
    }

    it should "create a final report for a given user" in {
      check(2, toIdx = Some(4), Some(userIdA))
    }

    it should "only include transaction metering after the from-date" in {
      check(12, None, None)
    }

    it should "only include transaction metering that existed before any to-timestamp" in {
      check(12, toIdx = Some(14), None)
    }

    it should "only include transaction metering for a given user" in {
      check(12, None, Some(userIdA))
    }

    it should "combine participant and transaction metering" in {
      check(2, None, None)
    }

    it should "combine participant and transaction metering where the to-date exceeds the aggregation timestamp" in {
      check(2, toIdx = Some(14), None)
    }

  }

}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.ApplicationId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  ReportData,
  TransactionMetering,
}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
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

    val appIdA: Ref.ApplicationId = Ref.ApplicationId.assertFromString("appA")
    val appIdB: Ref.ApplicationId = Ref.ApplicationId.assertFromString("appB")

    def buildTransactionMetering(
        index: Long,
        appId: Ref.ApplicationId,
        aggregated: Boolean = false,
    ) = TransactionMetering(
      applicationId = appId,
      actionCount = if (aggregated) 1000 else index.toInt, // Should not be used in tests below
      meteringTimestamp = someTime.addMicros(index),
      ledgerOffset = offset(index),
    )

    def buildParticipantMetering(index: Long, appId: Ref.ApplicationId) = ParticipantMetering(
      applicationId = appId,
      from = someTime.addMicros(index - 1),
      to = someTime.addMicros(index),
      actionCount = index.toInt,
      ledgerOffset = offset(index),
    )

    val participantMetering = Vector(
      buildParticipantMetering(1, appIdA),
      buildParticipantMetering(2, appIdA),
      buildParticipantMetering(3, appIdB),
      buildParticipantMetering(4, appIdA),
      buildParticipantMetering(5, appIdA),
    )
    discard(participantMetering)
    val ledgerMeteringEnd = LedgerMeteringEnd(offset(5L), someTime.addMicros(6L))
    val loggerFactory = SuppressingLogger(getClass)

    // Aggregated transaction metering should never be read
    val aggregatedTransactionMetering = Vector(
      buildTransactionMetering(1, appIdA, aggregated = true),
      buildTransactionMetering(2, appIdA, aggregated = true),
      buildTransactionMetering(3, appIdB, aggregated = true),
      buildTransactionMetering(4, appIdA, aggregated = true),
      buildTransactionMetering(5, appIdA, aggregated = true),
    )

    val transactionMetering = Vector(
      buildTransactionMetering(11, appIdA),
      buildTransactionMetering(12, appIdA),
      buildTransactionMetering(13, appIdB),
      buildTransactionMetering(14, appIdA),
      buildTransactionMetering(15, appIdA),
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
        applicationId: Option[ApplicationId],
    ): ReportData = {

      val participantMap =
        participantMetering
          .filter(_.from >= from)
          .filter(m => to.fold(true)(m.to <= _))
          .filter(m => applicationId.fold(true)(m.applicationId == _))
          .groupMapReduce(_.applicationId)(_.actionCount.toLong)(_ + _)

      val transactionMap =
        transactionMetering
          .filter(_.ledgerOffset > ledgerMeteringEnd.offset)
          .filter(m => to.fold(true)(m.meteringTimestamp < _))
          .filter(m => applicationId.fold(true)(m.applicationId == _))
          .groupMapReduce(_.applicationId)(_.actionCount.toLong)(_ + _)

      val apps: Set[ApplicationId] = participantMap.keySet ++ transactionMap.keySet

      val metering = apps.toList.map { a =>
        a -> (participantMap.getOrElse(a, 0L) + transactionMap.getOrElse(a, 0L))
      }.toMap

      val isFinal = to.fold(false)(ledgerMeteringEnd.timestamp >= _)

      ReportData(metering, isFinal)

    }

    def check(
        fromIdx: Long,
        toIdx: Option[Long],
        applicationId: Option[ApplicationId],
    ): Assertion = {
      populate()
      val from = someTime.addMicros(fromIdx)
      val to = toIdx.map(someTime.addMicros)
      val actual = executeSql(
        backend.metering.read.reportData(from, to, applicationId)
      )
      val expected = execute(from, to, applicationId)
      actual shouldBe expected
    }

    it should "create a final report if the aggregation timestamp exceeds the requested to-timestamp" in {
      check(2, toIdx = Some(4), None)
    }

    it should "create a final report for a given application" in {
      check(2, toIdx = Some(4), Some(appIdA))
    }

    it should "only include transaction metering after the from-date" in {
      check(12, None, None)
    }

    it should "only include transaction metering that existed before any to-timestamp" in {
      check(12, toIdx = Some(14), None)
    }

    it should "only include transaction metering for a given application" in {
      check(12, None, Some(appIdA))
    }

    it should "combine participant and transaction metering" in {
      check(2, None, None)
    }

    it should "combine participant and transaction metering where the to-date exceeds the aggregation timestamp" in {
      check(2, toIdx = Some(14), None)
    }

  }

}

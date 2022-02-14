// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.{
  ParticipantMetering,
  TransactionMetering,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside}

private[backend] trait StorageBackendTestsMetering
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues._

  {
    behavior of "StorageBackend (read metering)"

    it should "persist transaction metering" in {

      val toOffset = offset(5)

      val metering = TransactionMetering(
        someApplicationId,
        actionCount = 1,
        meteringTimestamp = someTime.addMicros(2),
        ledgerOffset = offset(4),
      )

      val expected = metering
      executeSql(backend.parameter.initializeParameters(someIdentityParams))
      executeSql(ingest(Vector(dtoTransactionMetering(metering)), _))
      executeSql(updateLedgerEnd(toOffset, 5L))
      val Vector(actual) =
        executeSql(backend.metering.read.transactionMetering(Timestamp.Epoch, None, None))
      actual shouldBe expected

    }

    val appIdA: Ref.ApplicationId = Ref.ApplicationId.assertFromString("appA")
    val appIdB: Ref.ApplicationId = Ref.ApplicationId.assertFromString("appB")

    def build(index: Long, appId: Ref.ApplicationId) = TransactionMetering(
      appId,
      actionCount = 1,
      meteringTimestamp = someTime.addMicros(index),
      ledgerOffset = offset(index),
    )

    val ledgerEnd = 4L

    val metering = Vector(
      build(1, appIdA),
      build(2, appIdA),
      build(3, appIdB),
      build(4, appIdA),
      build(5, appIdA),
    )

    def populate(): Unit = {
      executeSql(backend.parameter.initializeParameters(someIdentityParams))
      executeSql(ingest(metering.map(dtoTransactionMetering), _))
      executeSql(updateLedgerEnd(offset(ledgerEnd), ledgerEnd))
    }

    def execute(
        from: Timestamp,
        to: Option[Timestamp],
        applicationId: Option[ApplicationId],
    ): Set[TransactionMetering] = {
      metering
        .filter(_.ledgerOffset <= offset(ledgerEnd))
        .filter(_.meteringTimestamp >= from)
        .filter(m => to.fold(true)(m.meteringTimestamp < _))
        .filter(m => applicationId.fold(true)(e => m.applicationId == e))
        .toSet
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
        backend.metering.read.transactionMetering(from, to, applicationId)
      ).toSet
      val expected = execute(from, to, applicationId)
      actual.map(_.ledgerOffset) shouldBe expected.map(_.ledgerOffset)
      actual shouldBe expected
    }

    it should "only include after from date that have been ingested" in {
      check(2, None, None)
    }
    it should "only include rows that existed before any to timestamp" in {
      check(2, toIdx = Some(4), None)
    }
    it should "only include rows for any given application" in {
      check(2, None, Some(appIdA))
    }
  }

  {
    behavior of "StorageBackend (metering parameters)"

    val initLedgerMeteringEnd = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.Epoch)

    it should "fetch un-initialized ledger metering end" in {
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe None
    }

    it should "initialized ledger metering end" in {
      val expected = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.Epoch)
      executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(expected))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

    it should "update ledger metering end with `before begin` offset" in {
      executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(initLedgerMeteringEnd))
      val expected = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.now())
      executeSql(backend.meteringParameter.updateLedgerMeteringEnd(expected))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

    it should "update ledger metering end with valid offset" in {
      executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(initLedgerMeteringEnd))
      val expected = LedgerMeteringEnd(
        Offset.fromHexString(Ref.HexString.assertFromString("07")),
        Timestamp.now(),
      )
      executeSql(backend.meteringParameter.updateLedgerMeteringEnd(expected))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

  }

  {
    behavior of "StorageBackend (write metering)"

    val metering = Vector(7L, 8L, 9L, 10L).map { i =>
      TransactionMetering(
        someApplicationId,
        actionCount = 1,
        meteringTimestamp = someTime.addMicros(i),
        ledgerOffset = offset(i),
      )
    }

    val meteringOffsets = metering.map(_.ledgerOffset)
    val firstOffset = meteringOffsets.min
    val lastOffset = meteringOffsets.max
    val lastTime = metering.map(_.meteringTimestamp).max

    it should "return the maximum transaction metering offset" in {

      def check(from: Offset, to: Timestamp): Assertion = {
        val expected = metering
          .filter(_.ledgerOffset > from)
          .filter(_.meteringTimestamp < to)
          .map(_.ledgerOffset)
          .maxOption
        val actual = executeSql(backend.metering.write.transactionMeteringMaxOffset(from, to))
        actual shouldBe expected
      }

      executeSql(ingest(metering.map(dtoTransactionMetering), _))

      check(firstOffset, lastTime) // 9
      check(firstOffset, lastTime.addMicros(1)) // 10
      check(lastOffset, lastTime.addMicros(1)) // Unset

    }

    it should "select transaction metering for aggregation" in {

      executeSql(ingest(metering.map(dtoTransactionMetering), _))

      val nextLastOffset: Offset = meteringOffsets.filter(_ < lastOffset).max
      val expected = meteringOffsets.filter(_ > firstOffset).filter(_ <= nextLastOffset).toSet
      val actual = executeSql(
        backend.metering.write.transactionMetering(firstOffset, nextLastOffset)
      ).map(_.ledgerOffset).toSet
      actual shouldBe expected
    }

    it should "insert new participant metering records" in {

      val expected = Vector(7L, 8L, 9L).map { i =>
        ParticipantMetering(
          someApplicationId,
          someTime.addMicros(i),
          someTime.addMicros(i + 1),
          actionCount = 1,
          ledgerOffset = offset(i),
        )
      }

      executeSql(backend.metering.write.insertParticipantMetering(expected))
      val actual =
        executeSql(backend.metering.write.allParticipantMetering()).sortBy(_.ledgerOffset)
      actual shouldBe expected

    }

  }

}

// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  TransactionMetering,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside}

private[backend] trait StorageBackendTestsWriteMetering
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

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
        val actual = executeSql(backend.metering.write.transactionMeteringMaxOffset(Some(from), to))
        actual shouldBe expected
      }

      executeSql(ingest(metering.map(dtoTransactionMetering), _))

      check(firstOffset, lastTime) // 9
      check(firstOffset, lastTime.addMicros(1)) // 10
      check(lastOffset, lastTime.addMicros(1)) // Unset

    }

    it should "select transaction metering based on offsets" in {

      executeSql(ingest(metering.map(dtoTransactionMetering), _))

      val nextLastOffset: Offset = meteringOffsets.filter(_ < lastOffset).max
      val expected = metering
        .filter(_.ledgerOffset > firstOffset)
        .filter(_.ledgerOffset <= nextLastOffset)
        .groupMapReduce(_.applicationId)(_.actionCount)(_ + _)
      val actual = executeSql(
        backend.metering.write.selectTransactionMetering(
          Some(firstOffset),
          nextLastOffset,
        )
      )
      actual shouldBe expected
    }

    it should "delete transaction metering based on offsets" in {

      executeSql(ingest(metering.map(dtoTransactionMetering), _))

      val nextLastOffset: Offset = meteringOffsets.filter(_ < lastOffset).max

      executeSql(
        backend.metering.write
          .deleteTransactionMetering(
            from = Some(firstOffset),
            to = nextLastOffset,
          )
      )

      executeSql(
        backend.metering.write.selectTransactionMetering(
          from = Some(firstOffset),
          to = nextLastOffset,
        )
      ).size shouldBe 0

      executeSql(
        backend.metering.write.selectTransactionMetering(
          from = Some(firstOffset),
          to = lastOffset,
        )
      ).size shouldBe 1
    }

    it should "insert new participant metering records" in {

      val expected = Vector(7L, 8L, 9L).map { i =>
        ParticipantMetering(
          Ref.ApplicationId.assertFromString("App100"),
          someTime.addMicros(i),
          someTime.addMicros(i + 1),
          actionCount = 1,
          ledgerOffset = Some(offset(i)),
        )
      }

      executeSql(backend.metering.write.insertParticipantMetering(expected))
      val actual =
        executeSql(backend.metering.write.allParticipantMetering()).sortBy(_.ledgerOffset)
      actual shouldBe expected

    }

  }

}

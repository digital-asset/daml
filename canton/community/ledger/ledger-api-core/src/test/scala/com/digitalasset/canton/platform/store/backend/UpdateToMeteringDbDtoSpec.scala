// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.crypto.Hash
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.TransactionNodeStatistics.EmptyActions
import com.daml.lf.transaction.{
  CommittedTransaction,
  TransactionNodeStatistics,
  TransactionVersion,
  VersionedTransaction,
}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.metrics.IndexedUpdatesMetrics
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.scalatest.wordspec.AnyWordSpec

class UpdateToMeteringDbDtoSpec extends AnyWordSpec with MetricValues {

  import DbDtoEq.*

  import TraceContext.Implicits.Empty.*

  private val IndexedUpdatesMetrics = newUpdateMetrics

  "UpdateMeteringToDbDto" should {

    val applicationId = Ref.ApplicationId.assertFromString("a0")

    val timestamp: Long = 12345

    val offset = Ref.HexString.assertFromString("02")
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someRecordTime = Time.Timestamp.assertFromString("2000-01-01T00:00:00.000000Z")

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = Some(statistics),
    )
    val someTransactionMeta = state.TransactionMeta(
      ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
      workflowId = None,
      submissionTime = Time.Timestamp.assertFromLong(3),
      submissionSeed = someHash,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

    val someTransactionAccepted = state.Update.TransactionAccepted(
      completionInfoO = Some(someCompletionInfo),
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(
        VersionedTransaction(TransactionVersion.VDev, Map.empty, ImmArray.empty)
      ),
      transactionId = Ref.TransactionId.assertFromString("TransactionId"),
      recordTime = someRecordTime,
      divulgedContracts = List.empty,
      blindingInfoO = None,
      hostedWitnesses = Nil,
      Map.empty,
      domainId = DomainId.tryFromString("da::default"),
    )

    "extract transaction metering" in {

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, IndexedUpdatesMetrics)(MetricsContext.Empty)(
          List((Offset.fromHexString(offset), Traced[Update](someTransactionAccepted)))
        )

      val expected: Vector[DbDto.TransactionMetering] = Vector(
        DbDto.TransactionMetering(
          application_id = applicationId,
          action_count = statistics.committed.actions + statistics.rolledBack.actions,
          metering_timestamp = timestamp,
          ledger_offset = offset,
        )
      )

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

    "aggregate transaction metering across batch" in {

      val metering = DbDto.TransactionMetering(
        application_id = applicationId,
        action_count = 2 * (statistics.committed.actions + statistics.rolledBack.actions),
        metering_timestamp = timestamp,
        ledger_offset = Ref.HexString.assertFromString("99"),
      )

      val expected: Vector[DbDto.TransactionMetering] = Vector(metering)

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, IndexedUpdatesMetrics)(MetricsContext.Empty)(
          List(
            (
              Offset.fromHexString(Ref.HexString.assertFromString("01")),
              Traced[Update](someTransactionAccepted),
            ),
            (
              Offset.fromHexString(Ref.HexString.assertFromString(metering.ledger_offset)),
              Traced[Update](someTransactionAccepted),
            ),
          )
        )

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

    "return empty vector if input iterable is empty" in {
      val expected: Vector[DbDto.TransactionMetering] = Vector.empty
      val actual = UpdateToMeteringDbDto(clock = () => timestamp, IndexedUpdatesMetrics)(
        MetricsContext.Empty
      )(List.empty)
      actual should equal(expected)(decided by DbDtoSeqEq)
    }

    // This is so infrastructure transactions, with a zero action count, are not included
    "filter zero action counts" in {

      val txWithNoActionCount = someTransactionAccepted.copy(completionInfoO =
        Some(someCompletionInfo.copy(statistics = Some(TransactionNodeStatistics.Empty)))
      )

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, IndexedUpdatesMetrics)(MetricsContext.Empty)(
          List((Offset.fromHexString(offset), Traced[Update](txWithNoActionCount)))
        )

      actual.isEmpty shouldBe true
    }

    "increment metered events counter" in {
      val IndexedUpdatesMetrics = newUpdateMetrics
      UpdateToMeteringDbDto(clock = () => timestamp, IndexedUpdatesMetrics)(MetricsContext.Empty)(
        List((Offset.fromHexString(offset), Traced[Update](someTransactionAccepted)))
      )
      IndexedUpdatesMetrics.meteredEventsMeter.value shouldBe (statistics.committed.actions + statistics.rolledBack.actions)
    }
  }

  private def newUpdateMetrics = {
    new IndexedUpdatesMetrics(MetricName("test"), InMemoryMetricsFactory)
  }

}

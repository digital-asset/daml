// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex, Update}
import com.digitalasset.canton.metrics.{IndexerHistograms, IndexerMetrics}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.TransactionNodeStatistics.EmptyActions
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.transaction.{
  CommittedTransaction,
  NodeId,
  TransactionNodeStatistics,
  TransactionVersion,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class UpdateToMeteringDbDtoSpec extends AnyWordSpec with MetricValues {

  import DbDtoEq.*

  import TraceContext.Implicits.Empty.*

  implicit val inventory: HistogramInventory = new HistogramInventory()
  private val indexerHistograms = new IndexerHistograms(MetricName("test"))
  private val IndexedUpdatesMetrics = newUpdateMetrics(indexerHistograms)

  "UpdateMeteringToDbDto" should {

    val applicationId = Ref.ApplicationId.assertFromString("a0")

    val timestamp: Long = 12345

    val offset = Ref.HexString.assertFromString("02")
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someRecordTime =
      Time.Timestamp.assertFromInstant(Instant.parse("2000-01-01T00:00:00.000000Z"))

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
      messageUuid = None,
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

    def someContractNode = TestNodeBuilder.create(
      id = TransactionBuilder.newCid,
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString("abc"),
        Ref.QualifiedName.assertFromString("Main:Template"),
      ),
      argument = Value.ValueUnit,
      signatories = Set.empty,
      observers = Set.empty,
    )
    val someConsumingExerciseNode = TestNodeBuilder.exercise(
      contract = someContractNode,
      choice = Ref.Name.assertFromString("somechoice"),
      consuming = true,
      actingParties = Set.empty,
      argument = Value.ValueUnit,
      byKey = false,
    )
    val someTransactionAccepted = state.Update.TransactionAccepted(
      completionInfoO = Some(someCompletionInfo),
      transactionMeta = someTransactionMeta,
      transaction = TransactionBuilder.justCommitted(
        someContractNode,
        someContractNode,
        someConsumingExerciseNode,
        TestNodeBuilder.rollback(
          ImmArray(
            NodeId(2)
          )
        ),
      ),
      updateId = Ref.TransactionId.assertFromString("UpdateId"),
      recordTime = someRecordTime,
      hostedWitnesses = Nil,
      Map.empty,
      domainId = DomainId.tryFromString("da::default"),
      domainIndex =
        Some(DomainIndex.of(RequestIndex(RequestCounter(10), None, CantonTimestamp.now()))),
    )

    "extract transaction metering" in {

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
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
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
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
      val actual = UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
        MetricsContext.Empty
      )(List.empty)
      actual should equal(expected)(decided by DbDtoSeqEq)
    }

    // This is so infrastructure transactions, with a zero action count, are not included
    "filter zero action counts" in {

      val txWithNoActionCount = someTransactionAccepted.copy(
        transaction = CommittedTransaction(
          VersionedTransaction(TransactionVersion.VDev, Map.empty, ImmArray.empty)
        )
      )

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
          List((Offset.fromHexString(offset), Traced[Update](txWithNoActionCount)))
        )

      actual.isEmpty shouldBe true
    }

    "increment metered events counter" in {
      val indexerMetrics = newUpdateMetrics(indexerHistograms)
      UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, indexerMetrics)(
        MetricsContext.Empty
      )(
        List((Offset.fromHexString(offset), Traced[Update](someTransactionAccepted)))
      )
      indexerMetrics.meteredEventsMeter.value shouldBe (statistics.committed.actions + statistics.rolledBack.actions)
    }
  }

  private def newUpdateMetrics(indexerHistograms: IndexerHistograms) =
    new IndexerMetrics(indexerHistograms, InMemoryMetricsFactory)

}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.metrics.{IndexerHistograms, IndexerMetrics}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.TransactionNodeStatistics.EmptyActions
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.transaction.{
  CommittedTransaction,
  NodeId,
  TransactionNodeStatistics,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class UpdateToMeteringDbDtoSpec extends AnyWordSpec with MetricValues {

  import DbDtoEq.*

  import TraceContext.Implicits.Empty.*

  implicit val inventory: HistogramInventory = new HistogramInventory()
  private val indexerHistograms = new IndexerHistograms(MetricName("test"))
  private val IndexedUpdatesMetrics = newUpdateMetrics(indexerHistograms)

  "UpdateMeteringToDbDto" should {

    val applicationId = Ref.ApplicationId.assertFromString("a0")

    val timestamp: Long = 12345

    val offset = AbsoluteOffset.tryFromLong(2L)
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
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
    val someTransactionAccepted = state.Update.SequencedTransactionAccepted(
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
      hostedWitnesses = Nil,
      Map.empty,
      domainId = DomainId.tryFromString("da::default"),
      RequestCounter(10),
      SequencerCounter(10),
      CantonTimestamp.now(),
    )

    "extract transaction metering" in {

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
          List((offset, someTransactionAccepted))
        )

      val expected: Vector[DbDto.TransactionMetering] = Vector(
        DbDto.TransactionMetering(
          application_id = applicationId,
          action_count = statistics.committed.actions + statistics.rolledBack.actions,
          metering_timestamp = timestamp,
          ledger_offset = offset.toHexString,
        )
      )

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

    "aggregate transaction metering across batch" in {

      val metering = DbDto.TransactionMetering(
        application_id = applicationId,
        action_count = 2 * (statistics.committed.actions + statistics.rolledBack.actions),
        metering_timestamp = timestamp,
        ledger_offset = Ref.HexString.assertFromString("00" * 8 + "99"),
      )

      val expected: Vector[DbDto.TransactionMetering] = Vector(metering)

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
          List(
            (
              AbsoluteOffset.tryFromLong(1L),
              someTransactionAccepted,
            ),
            (
              Offset
                .fromHexString(Ref.HexString.assertFromString(metering.ledger_offset))
                .toAbsoluteOffset,
              someTransactionAccepted,
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
          VersionedTransaction(LanguageVersion.v2_dev, Map.empty, ImmArray.empty)
        )
      )

      val actual =
        UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, IndexedUpdatesMetrics)(
          MetricsContext.Empty
        )(
          List((offset, txWithNoActionCount))
        )

      actual.isEmpty shouldBe true
    }

    "increment metered events counter" in {
      val indexerMetrics = newUpdateMetrics(indexerHistograms)
      UpdateToMeteringDbDto(clock = () => timestamp, Set.empty, indexerMetrics)(
        MetricsContext.Empty
      )(
        List((offset, someTransactionAccepted))
      )
      indexerMetrics.meteredEventsMeter.value shouldBe (statistics.committed.actions + statistics.rolledBack.actions)
    }
  }

  private def newUpdateMetrics(indexerHistograms: IndexerHistograms) =
    new IndexerMetrics(indexerHistograms, InMemoryMetricsFactory)

}

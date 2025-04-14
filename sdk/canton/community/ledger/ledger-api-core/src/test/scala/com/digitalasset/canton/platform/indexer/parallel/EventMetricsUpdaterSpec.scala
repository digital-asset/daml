// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.metrics.api.testing.MetricValues
import com.daml.metrics.api.{MetricHandle, MetricsContext}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
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
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, verify, verifyZeroInteractions}
import org.mockito.captor.ArgCaptor
import org.scalatest.wordspec.AnyWordSpec

class EventMetricsUpdaterSpec extends AnyWordSpec with MetricValues {

  import TraceContext.Implicits.Empty.*

  "EventMetricsUpdater" should {

    val userId = Ref.UserId.assertFromString("a0")

    val offset = Offset.tryFromLong(2L)
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      userId = userId,
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
      Map.empty,
      synchronizerId = SynchronizerId.tryFromString("da::default"),
      CantonTimestamp.now(),
    )

    "extract transaction metering" in {

      val meter: MetricHandle.Meter = mock[MetricHandle.Meter]
      val captor = ArgCaptor[Long]

      EventMetricsUpdater(meter)(
        MetricsContext.Empty
      )(
        List((offset, someTransactionAccepted))
      )

      verify(meter).mark(captor)(any[MetricsContext])
      captor hasCaptured (statistics.committed.actions + statistics.rolledBack.actions).toLong
    }

    "aggregate transaction metering across batch" in {

      val meter: MetricHandle.Meter = mock[MetricHandle.Meter]
      val captor = ArgCaptor[Long]

      EventMetricsUpdater(meter)(
        MetricsContext.Empty
      )(
        List(
          (
            Offset.tryFromLong(1L),
            someTransactionAccepted,
          ),
          (
            Offset.tryFromLong(2L),
            someTransactionAccepted,
          ),
        )
      )

      verify(meter).mark(captor)(any[MetricsContext])
      captor hasCaptured 2 * (statistics.committed.actions + statistics.rolledBack.actions).toLong
    }

    "no metrics if input iterable is empty" in {
      val meter: MetricHandle.Meter = mock[MetricHandle.Meter]
      EventMetricsUpdater(meter)(
        MetricsContext.Empty
      )(List.empty)
      verifyZeroInteractions(meter)
    }

    "no metrics for infrastructure transactions" in {

      val meter: MetricHandle.Meter = mock[MetricHandle.Meter]
      val txWithNoActionCount = someTransactionAccepted.copy(
        transaction = CommittedTransaction(
          VersionedTransaction(LanguageVersion.v2_dev, Map.empty, ImmArray.empty)
        )
      )

      EventMetricsUpdater(meter)(
        MetricsContext.Empty
      )(
        List((offset, txWithNoActionCount))
      )

      verifyZeroInteractions(meter)
    }
  }
}

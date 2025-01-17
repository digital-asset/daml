// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
  SynchronizerParameters,
  TestSynchronizerParameters,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SortedReconciliationIntervalsHelpers {
  this: BaseTest =>
  protected val defaultParameters = TestSynchronizerParameters.defaultDynamic
  protected val defaultReconciliationInterval = defaultParameters.reconciliationInterval
  private lazy val defaultSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::default")
  )

  protected def mkDynamicSynchronizerParameters(
      validFrom: Long,
      validTo: Long,
      reconciliationInterval: Long,
      protocolVersion: ProtocolVersion,
  ): DynamicSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      DynamicSynchronizerParameters.tryInitialValues(
        topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
        reconciliationInterval = PositiveSeconds.tryOfSeconds(reconciliationInterval),
        protocolVersion = protocolVersion,
      ),
      fromEpoch(validFrom),
      Some(fromEpoch(validTo)),
      defaultSynchronizerId,
    )

  protected def mkDynamicSynchronizerParameters(
      validFrom: Long,
      reconciliationInterval: Long,
      protocolVersion: ProtocolVersion,
  ): DynamicSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      DynamicSynchronizerParameters.tryInitialValues(
        topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
        reconciliationInterval = PositiveSeconds.tryOfSeconds(reconciliationInterval),
        protocolVersion = protocolVersion,
      ),
      fromEpoch(validFrom),
      None,
      defaultSynchronizerId,
    )

  protected def mkParameters(
      validFrom: CantonTimestamp,
      validTo: CantonTimestamp,
      reconciliationInterval: Long,
  ): SynchronizerParameters.WithValidity[PositiveSeconds] =
    SynchronizerParameters.WithValidity(
      validFrom,
      Some(validTo),
      PositiveSeconds.tryOfSeconds(reconciliationInterval),
    )

  protected def mkParameters(
      validFrom: CantonTimestamp,
      reconciliationInterval: Long,
  ): SynchronizerParameters.WithValidity[PositiveSeconds] =
    SynchronizerParameters.WithValidity(
      validFrom,
      None,
      PositiveSeconds.tryOfSeconds(reconciliationInterval),
    )

  protected def mkDynamicSynchronizerParameters(
      validFrom: CantonTimestamp,
      reconciliationInterval: PositiveSeconds,
  ): DynamicSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      defaultParameters.tryUpdate(reconciliationInterval = reconciliationInterval),
      validFrom,
      None,
      defaultSynchronizerId,
    )

  protected def fromEpoch(seconds: Long): CantonTimestamp =
    CantonTimestamp.Epoch + NonNegativeFiniteDuration.tryOfSeconds(seconds)

  protected def fromEpochSecond(seconds: Long): CantonTimestampSecond =
    CantonTimestampSecond.ofEpochSecond(seconds)

  protected def mkCommitmentPeriod(times: (Long, Long)): CommitmentPeriod = {
    val (after, beforeAndAt) = times

    CommitmentPeriod
      .create(
        CantonTimestampSecond.ofEpochSecond(after),
        CantonTimestampSecond.ofEpochSecond(beforeAndAt),
      )
      .value
  }

  /** Creates a SortedReconciliationIntervalsProvider that returns
    * always the same reconciliation interval
    *
    * @param synchronizerBootstrappingTime `validFrom` time of the synchronizer parameters
    */
  protected def constantSortedReconciliationIntervalsProvider(
      reconciliationInterval: PositiveSeconds,
      synchronizerBootstrappingTime: CantonTimestamp = CantonTimestamp.MinValue,
  )(implicit executionContext: ExecutionContext): SortedReconciliationIntervalsProvider = {

    val topologyClient = mock[SynchronizerTopologyClient]
    val topologySnapshot = mock[TopologySnapshot]

    when(topologyClient.approximateTimestamp).thenReturn(CantonTimestamp.MaxValue)
    when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn(
      FutureUnlessShutdown.pure(topologySnapshot)
    )

    when(topologySnapshot.listDynamicSynchronizerParametersChanges()).thenReturn {
      FutureUnlessShutdown.pure(
        Seq(
          mkDynamicSynchronizerParameters(synchronizerBootstrappingTime, reconciliationInterval)
        )
      )
    }

    new SortedReconciliationIntervalsProvider(
      topologyClient = topologyClient,
      futureSupervisor = FutureSupervisor.Noop,
      loggerFactory = loggerFactory,
    )
  }

}

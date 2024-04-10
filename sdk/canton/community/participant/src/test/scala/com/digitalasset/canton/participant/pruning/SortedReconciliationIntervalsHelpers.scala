// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.protocol.{
  DomainParameters,
  DynamicDomainParameters,
  DynamicDomainParametersWithValidity,
  TestDomainParameters,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait SortedReconciliationIntervalsHelpers {
  this: BaseTest =>
  protected val defaultParameters = TestDomainParameters.defaultDynamic
  protected val defaultReconciliationInterval = defaultParameters.reconciliationInterval
  private lazy val defaultDomainId = DomainId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::default")
  )

  protected def mkDynamicDomainParameters(
      validFrom: Long,
      validTo: Long,
      reconciliationInterval: Long,
      protocolVersion: ProtocolVersion,
  ): DynamicDomainParametersWithValidity =
    DynamicDomainParametersWithValidity(
      DynamicDomainParameters.tryInitialValues(
        topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
        reconciliationInterval = PositiveSeconds.tryOfSeconds(reconciliationInterval),
        protocolVersion = protocolVersion,
      ),
      fromEpoch(validFrom),
      Some(fromEpoch(validTo)),
      defaultDomainId,
    )

  protected def mkDynamicDomainParameters(
      validFrom: Long,
      reconciliationInterval: Long,
      protocolVersion: ProtocolVersion,
  ): DynamicDomainParametersWithValidity =
    DynamicDomainParametersWithValidity(
      DynamicDomainParameters.tryInitialValues(
        topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
        reconciliationInterval = PositiveSeconds.tryOfSeconds(reconciliationInterval),
        protocolVersion = protocolVersion,
      ),
      fromEpoch(validFrom),
      None,
      defaultDomainId,
    )

  protected def mkParameters(
      validFrom: CantonTimestamp,
      validTo: CantonTimestamp,
      reconciliationInterval: Long,
  ): DomainParameters.WithValidity[PositiveSeconds] =
    DomainParameters.WithValidity(
      validFrom,
      Some(validTo),
      PositiveSeconds.tryOfSeconds(reconciliationInterval),
    )

  protected def mkParameters(
      validFrom: CantonTimestamp,
      reconciliationInterval: Long,
  ): DomainParameters.WithValidity[PositiveSeconds] =
    DomainParameters.WithValidity(
      validFrom,
      None,
      PositiveSeconds.tryOfSeconds(reconciliationInterval),
    )

  protected def mkDynamicDomainParameters(
      validFrom: CantonTimestamp,
      reconciliationInterval: PositiveSeconds,
  ): DynamicDomainParametersWithValidity =
    DynamicDomainParametersWithValidity(
      defaultParameters.tryUpdate(reconciliationInterval = reconciliationInterval),
      validFrom,
      None,
      defaultDomainId,
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
    * @param domainBootstrappingTime `validFrom` time of the domain parameters
    */
  protected def constantSortedReconciliationIntervalsProvider(
      reconciliationInterval: PositiveSeconds,
      domainBootstrappingTime: CantonTimestamp = CantonTimestamp.MinValue,
  )(implicit executionContext: ExecutionContext): SortedReconciliationIntervalsProvider = {

    val topologyClient = mock[DomainTopologyClient]
    val topologySnapshot = mock[TopologySnapshot]

    when(topologyClient.approximateTimestamp).thenReturn(CantonTimestamp.MaxValue)
    when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn(
      Future.successful(topologySnapshot)
    )

    when(topologySnapshot.listDynamicDomainParametersChanges()).thenReturn {
      Future.successful(
        Seq(
          mkDynamicDomainParameters(domainBootstrappingTime, reconciliationInterval)
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

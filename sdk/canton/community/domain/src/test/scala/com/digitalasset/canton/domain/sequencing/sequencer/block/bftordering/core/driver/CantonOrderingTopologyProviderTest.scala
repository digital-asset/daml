// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.BaseTest.testedProtocolVersion
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{
  DynamicSequencingParameters,
  DynamicSequencingParametersWithValidity,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.{
  SequencerGroup,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class CantonOrderingTopologyProviderTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  import CantonOrderingTopologyProviderTest.*

  "Getting the ordering topology" should {
    "return the correct ordering topology" in {
      val topologySnapshotMock = mock[TopologySnapshot]
      when(topologySnapshotMock.timestamp).thenReturn(aTimestamp)
      when(topologySnapshotMock.sequencerGroup())
        .thenReturn(
          FutureUnlessShutdown.pure(
            Some(SequencerGroup(someSequencerIds, Seq.empty, PositiveInt.one))
          )
        )
      when(topologySnapshotMock.memberFirstKnownAt(any[SequencerId])(any[TraceContext]))
        .thenReturn(
          FutureUnlessShutdown.pure(Some((SequencedTime(aTimestamp), EffectiveTime(aTimestamp))))
        )
      when(topologySnapshotMock.findDynamicSequencingParameters()(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Right(someDynamicSequencingParameters)))
      val domainSnapshotSyncCryptoApiMock = mock[DomainSnapshotSyncCryptoApi]
      when(domainSnapshotSyncCryptoApiMock.ipsSnapshot).thenReturn(topologySnapshotMock)
      val cryptoApiMock = mock[DomainSyncCryptoClient]
      when(cryptoApiMock.awaitSnapshotUS(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(domainSnapshotSyncCryptoApiMock))

      Table(
        (
          "activationTimestamp",
          "maxEffectiveTimestamp",
          "expected pending topology changes true/false",
        ),
        // Case 1: the topology snapshot query timestamp is the immediate successor of the maximum effective timestamp,
        //  i.e., the maximum activation timestamp is the same as the topology snapshot query timestamp; e.g.,
        //  a valid topology transaction is the last sequenced event and topology change delay is 0, then
        //  no topology changes are pending because the last topology change is already active at the topology
        //  snapshot query time.
        (CantonTimestamp.MinValue.immediateSuccessor, CantonTimestamp.MinValue, false),
        // Case 2: the topology snapshot query timestamp equals the maximum effective timestamp,
        //  i.e., the maximum activation timestamp is 1 microsecond later than the topology snapshot query timestamp;
        //  e.g., a valid topology transaction is the last sequenced event and topology change delay is 1 microsecond,
        //  then the corresponding topology change is pending because it is not already active at the topology
        //  snapshot query time.
        (CantonTimestamp.MinValue, CantonTimestamp.MinValue, true),
      ).forEvery {
        case (activationTimestamp, maxEffectiveTimestamp, expectedPendingTopologyChangesFlag) =>
          when(cryptoApiMock.awaitMaxTimestampUS(activationTimestamp))
            .thenReturn(
              FutureUnlessShutdown.pure(
                Some((SequencedTime(aTimestamp), EffectiveTime(maxEffectiveTimestamp)))
              )
            )
          val cantonOrderingTopologyProvider =
            new CantonOrderingTopologyProvider(cryptoApiMock, loggerFactory)
          cantonOrderingTopologyProvider
            .getOrderingTopologyAt(TopologyActivationTime(activationTimestamp))
            .futureUnlessShutdown
            .futureValueUS should matchPattern {
            case Some((OrderingTopology(_, _, _, `expectedPendingTopologyChangesFlag`), _)) =>
          }
      }
    }
  }
}

object CantonOrderingTopologyProviderTest {

  private val aTimestamp = CantonTimestamp.MinValue
  private val someSequencerIds = Seq(fakeSequencerId("1"), fakeSequencerId("2"))
  private val someDynamicSequencingParameters =
    DynamicSequencingParametersWithValidity(
      DynamicSequencingParameters(None)(
        DynamicSequencingParameters.protocolVersionRepresentativeFor(testedProtocolVersion)
      ),
      validFrom = aTimestamp,
      validUntil = None,
      synchronizerId = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    )
}

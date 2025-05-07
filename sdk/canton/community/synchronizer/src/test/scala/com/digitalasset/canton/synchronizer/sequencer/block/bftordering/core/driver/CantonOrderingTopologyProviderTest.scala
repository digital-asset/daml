// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.BaseTest.testedProtocolVersion
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SigningKeySpec.EcSecp256k1
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  CryptoTestHelper,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{
  DynamicSequencingParameters,
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.sequencing.protocol.MaxRequestSizeToDeserialize
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.CantonCryptoProvider.BftOrderingSigningKeyUsage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class CantonOrderingTopologyProviderTest
    extends AsyncWordSpec
    with CryptoTestHelper
    with BaseTest
    with HasExecutionContext {

  import CantonOrderingTopologyProviderTest.*

  "Getting the ordering topology" should {
    "return the correct ordering topology" in {
      val crypto = SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
      val pk = getSigningPublicKey(crypto, BftOrderingSigningKeyUsage, EcSecp256k1).futureValueUS
      val topologySnapshotMock = mock[TopologySnapshot]
      when(topologySnapshotMock.timestamp).thenReturn(aTimestamp)
      when(topologySnapshotMock.sequencerGroup())
        .thenReturn(
          FutureUnlessShutdown.pure(
            Some(SequencerGroup(someSequencerIds, Seq.empty, PositiveInt.one))
          )
        )
      when(topologySnapshotMock.findDynamicSynchronizerParameters())
        .thenReturn(
          FutureUnlessShutdown.pure(
            Right(
              DynamicSynchronizerParametersWithValidity(
                DynamicSynchronizerParameters
                  .defaultValues(
                    testedProtocolVersion
                  ),
                validFrom = aTimestamp,
                validUntil = None,
                synchronizerId =
                  SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
              )
            ).withLeft[String]
          )
        )
      when(topologySnapshotMock.memberFirstKnownAt(any[SequencerId])(any[TraceContext]))
        .thenReturn(
          FutureUnlessShutdown.pure(Some((SequencedTime(aTimestamp), EffectiveTime(aTimestamp))))
        )
      when(topologySnapshotMock.findDynamicSequencingParameters()(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Right(someDynamicSequencingParameters)))
      when(
        topologySnapshotMock.signingKeys(eqTo(someSequencerIds), eqTo(BftOrderingSigningKeyUsage))(
          any[TraceContext]
        )
      )
        .thenReturn(
          FutureUnlessShutdown.pure(
            someSequencerIds.map(sequencerId => sequencerId -> Seq(pk)).toMap
          )
        )
      val synchronizerSnapshotSyncCryptoApiMock = mock[SynchronizerSnapshotSyncCryptoApi]
      when(synchronizerSnapshotSyncCryptoApiMock.ipsSnapshot).thenReturn(topologySnapshotMock)
      val cryptoApiMock = mock[SynchronizerCryptoClient]
      when(cryptoApiMock.awaitSnapshot(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(synchronizerSnapshotSyncCryptoApiMock))

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
        (aTimestamp.immediateSuccessor, aTimestamp, false),
        // Case 2: the topology snapshot query timestamp equals the maximum effective timestamp,
        //  i.e., the maximum activation timestamp is 1 microsecond later than the topology snapshot query timestamp;
        //  e.g., a valid topology transaction is the last sequenced event and topology change delay is 1 microsecond,
        //  then the corresponding topology change is pending because it is not already active at the topology
        //  snapshot query time.
        (aTimestamp, aTimestamp, true),
      ).forEvery {
        case (activationTimestamp, maxEffectiveTimestamp, expectedPendingTopologyChangesFlag) =>
          when(
            cryptoApiMock.awaitMaxTimestamp(SequencedTime(activationTimestamp.immediatePredecessor))
          )
            .thenReturn(
              FutureUnlessShutdown.pure(
                Some((SequencedTime(aTimestamp), EffectiveTime(maxEffectiveTimestamp)))
              )
            )
          new CantonOrderingTopologyProvider(cryptoApiMock, loggerFactory)
            .getOrderingTopologyAt(TopologyActivationTime(activationTimestamp))
            .futureUnlessShutdown()
            .futureValueUS
            .fold(fail("Ordering topology not returned")) { case (orderingTopology, _) =>
              orderingTopology.areTherePendingCantonTopologyChanges shouldBe expectedPendingTopologyChangesFlag
              orderingTopology.nodesTopologyInfo should contain theSameElementsAs someSequencerIds
                .map(sequencerId =>
                  SequencerNodeId.toBftNodeId(sequencerId) -> NodeTopologyInfo(
                    activationTime =
                      TopologyActivationTime.fromEffectiveTime(EffectiveTime(aTimestamp)),
                    keyIds = Set(FingerprintKeyId.toBftKeyId(pk.id)),
                  )
                )
              orderingTopology.maxRequestSizeToDeserialize shouldBe
                MaxRequestSizeToDeserialize.Limit(
                  DynamicSynchronizerParameters.defaultMaxRequestSize.value
                )
            }
      }
    }
  }
}

object CantonOrderingTopologyProviderTest {

  private def fakeSequencerId(name: String): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate("ns", s"fake_$name"))

  private val aTimestamp = CantonTimestamp.Epoch
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

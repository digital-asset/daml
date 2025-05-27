// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.sequencer.admin.v30.{
  AddPeerEndpointRequest,
  GetOrderingTopologyRequest,
  GetPeerNetworkStatusRequest,
  PeerEndpoint,
  PeerEndpointId,
  RemovePeerEndpointRequest,
  TlsPeerEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.PeerNetworkStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  TlsP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.fakeIgnoringModule
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Promise

class BftOrderingSequencerAdminServiceTest extends AsyncWordSpec with BftSequencerBaseTest {

  "BftOrderingSequencerAdminService" should {
    "delegate addPeerEndpoint to the p2p out module" in {
      val p2PNetworkOutAdminSpy =
        spy[ModuleRef[P2PNetworkOut.Admin]](fakeIgnoringModule[P2PNetworkOut.Admin])
      val consensusAdminSpy =
        spy[ModuleRef[Consensus.Admin]](fakeIgnoringModule[Consensus.Admin])
      val resultPromise = Promise[Boolean]()
      resultPromise.success(true)
      val bftOrderingSequencerAdminService =
        new BftOrderingSequencerAdminService(
          p2PNetworkOutAdminSpy,
          consensusAdminSpy,
          loggerFactory,
          () => resultPromise,
        )
      bftOrderingSequencerAdminService
        .addPeerEndpoint(
          AddPeerEndpointRequest(
            Some(
              PeerEndpoint(
                "localhost",
                port = 1234,
                PeerEndpoint.Security.Tls(
                  TlsPeerEndpoint(customServerTrustCertificate = None, clientCertificate = None)
                ),
              )
            )
          )
        )
        .map { response =>
          verify(p2PNetworkOutAdminSpy).asyncSend(
            P2PNetworkOut.Admin
              .AddEndpoint(
                TlsP2PEndpoint(
                  P2PEndpointConfig("localhost", Port.tryCreate(1234), tlsConfig = None)
                ),
                any[Boolean => Unit],
              )
          )(any[MetricsContext])
          response.added shouldBe true
        }
    }
  }

  "BftOrderingSequencerAdminService" should {
    "delegate removePeerEndpoint to the p2p out module" in {
      val p2PNetworkOutAdminSpy =
        spy[ModuleRef[P2PNetworkOut.Admin]](fakeIgnoringModule[P2PNetworkOut.Admin])
      val consensusAdminSpy =
        spy[ModuleRef[Consensus.Admin]](fakeIgnoringModule[Consensus.Admin])
      val resultPromise = Promise[Boolean]()
      resultPromise.success(true)
      val bftOrderingSequencerAdminService =
        new BftOrderingSequencerAdminService(
          p2PNetworkOutAdminSpy,
          consensusAdminSpy,
          loggerFactory,
          () => resultPromise,
        )
      bftOrderingSequencerAdminService
        .removePeerEndpoint(
          RemovePeerEndpointRequest(
            Some(
              PeerEndpointId(
                "localhost",
                port = 1234,
                tls = true,
              )
            )
          )
        )
        .map { response =>
          verify(p2PNetworkOutAdminSpy).asyncSend(
            P2PNetworkOut.Admin
              .RemoveEndpoint(
                P2PEndpoint.Id("localhost", Port.tryCreate(1234), transportSecurity = true),
                any[Boolean => Unit],
              )
          )(any[MetricsContext])
          response.removed shouldBe true
        }
    }
  }

  "BftOrderingSequencerAdminService" should {
    "delegate getPeerNetworkStatus to the p2p out module" in {
      val p2PNetworkOutAdminSpy =
        spy[ModuleRef[P2PNetworkOut.Admin]](fakeIgnoringModule[P2PNetworkOut.Admin])
      val consensusAdminSpy =
        spy[ModuleRef[Consensus.Admin]](fakeIgnoringModule[Consensus.Admin])
      val resultPromise = Promise[PeerNetworkStatus]()
      resultPromise.success(PeerNetworkStatus(Seq.empty))
      val bftOrderingSequencerAdminService =
        new BftOrderingSequencerAdminService(
          p2PNetworkOutAdminSpy,
          consensusAdminSpy,
          loggerFactory,
          createNetworkStatusPromise = () => resultPromise,
        )
      bftOrderingSequencerAdminService
        .getPeerNetworkStatus(GetPeerNetworkStatusRequest(Seq.empty))
        .map { response =>
          verify(p2PNetworkOutAdminSpy).asyncSend(
            P2PNetworkOut.Admin.GetStatus(any[PeerNetworkStatus => Unit])
          )(any[MetricsContext])
          response.statuses shouldBe empty
        }
    }
  }

  "BftOrderingSequencerAdminService" should {
    "delegate getOrderingTopology to the consensus module" in {
      val p2PNetworkOutAdminSpy =
        spy[ModuleRef[P2PNetworkOut.Admin]](fakeIgnoringModule[P2PNetworkOut.Admin])
      val consensusAdminSpy =
        spy[ModuleRef[Consensus.Admin]](fakeIgnoringModule[Consensus.Admin])
      val resultPromise = Promise[(EpochNumber, Set[BftNodeId])]()
      resultPromise.success(EpochNumber.First -> Set.empty)
      val bftOrderingSequencerAdminService =
        new BftOrderingSequencerAdminService(
          p2PNetworkOutAdminSpy,
          consensusAdminSpy,
          loggerFactory,
          createOrderingTopologyPromise = () => resultPromise,
        )
      bftOrderingSequencerAdminService
        .getOrderingTopology(GetOrderingTopologyRequest())
        .map { response =>
          verify(consensusAdminSpy).asyncSend(
            Consensus.Admin.GetOrderingTopology(any[(EpochNumber, Set[BftNodeId]) => Unit])
          )(any[MetricsContext])
          response.sequencerIds shouldBe empty
        }
    }
  }
}

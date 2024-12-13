// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.bftordering.admin

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.BftOrderingSequencerAdminService
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.SequencerBftAdminData.PeerNetworkStatus
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  P2PNetworkOut,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.fakeIgnoringModule
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencer.admin.v30.{
  AddPeerEndpointRequest,
  GetOrderingTopologyRequest,
  GetPeerNetworkStatusRequest,
  PeerEndpoint,
  RemovePeerEndpointRequest,
}
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Promise

class BftOrderingSequencerAdminServiceTest extends AsyncWordSpec with BaseTest {

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
        .addPeerEndpoint(AddPeerEndpointRequest(Some(PeerEndpoint("localhost", 1234))))
        .map { response =>
          verify(p2PNetworkOutAdminSpy).asyncSend(
            P2PNetworkOut.Admin
              .AddEndpoint(new Endpoint("localhost", Port.tryCreate(1234)), any[Boolean => Unit])
          )
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
        .removePeerEndpoint(RemovePeerEndpointRequest(Some(PeerEndpoint("localhost", 1234))))
        .map { response =>
          verify(p2PNetworkOutAdminSpy).asyncSend(
            P2PNetworkOut.Admin
              .RemoveEndpoint(new Endpoint("localhost", Port.tryCreate(1234)), any[Boolean => Unit])
          )
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
            P2PNetworkOut.Admin
              .GetStatus(None, any[PeerNetworkStatus => Unit])
          )
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
      val resultPromise = Promise[(EpochNumber, Set[SequencerId])]()
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
            Consensus.Admin.GetOrderingTopology(any[(EpochNumber, Set[SequencerId]) => Unit])
          )
          response.sequencerIds shouldBe empty
        }
    }
  }
}

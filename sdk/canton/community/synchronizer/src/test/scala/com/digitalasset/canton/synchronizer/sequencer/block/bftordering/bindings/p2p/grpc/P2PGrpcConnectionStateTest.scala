// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import io.grpc.stub.StreamObserver
import org.mockito.MockitoSugar.mock
import org.scalatest.wordspec.AnyWordSpec

class P2PGrpcConnectionStateTest extends AnyWordSpec with BftSequencerBaseTest {

  import P2PGrpcConnectionStateTest.*

  "P2PGrpcConnectionState" should {

    "associate multiple P2P endpoint ID to a BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId)
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false

      state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)

      state.connections should contain theSameElementsAs Seq(
        Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
        Some(AnotherPeerP2PEndpoint.id) -> Some(APeerBftNodeId),
      )
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe false
    }

    "reject associating a P2P endpoint ID to the self BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      suppressProblemLogs(
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, SelfBftNodeId) shouldBe Left(
          P2PConnectionState.Error
            .CannotAssociateP2PEndpointIdsToSelf(APeerP2PEndpoint.id, SelfBftNodeId)
        )
      )

      state.connections shouldBe empty
      state.isDefined(APeerP2PEndpoint.id) shouldBe false
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
    }

    "reject associating a P2P endpoint ID to a BFT node ID if already associated to another BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      suppressProblemLogs(
        state.associateP2PEndpointIdToBftNodeId(
          APeerP2PEndpoint.id,
          AnotherPeerBftNodeId,
        ) shouldBe Left(
          P2PConnectionState.Error
            .P2PEndpointIdAlreadyAssociated(
              APeerP2PEndpoint.id,
              APeerBftNodeId,
              AnotherPeerBftNodeId,
            )
        )
      )

      state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId)
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
    }

    "associate a gRPC streaming sender with a BFT node ID only if missing" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.getSender(APeerP2PNodeAddressId) shouldBe None

      state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true

      state.getSender(APeerP2PNodeAddressId) shouldBe Some(ASender)

      state.addSenderIfMissing(APeerBftNodeId, AnotherSender) shouldBe false

      state.getSender(APeerP2PNodeAddressId) shouldBe Some(ASender)
    }

    "associate a network ref with an address ID only if missing" in {
      Table("address ID", APeerP2PNodeAddressId, APeerP2PEndpointAddressId).forEvery { addressId =>
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        def checkP2PAddressState(
            isEndpointPresent: Boolean,
            maybeNetworkRef: Option[P2PNetworkRef[BftOrderingMessage]],
        ) =
          addressId match {
            case Left(endpoint) =>
              state.isDefined(endpoint) shouldBe isEndpointPresent
              state.isOutgoing(endpoint) shouldBe isEndpointPresent
            case Right(nodeId) =>
              state.getNetworkRef(nodeId) shouldBe maybeNetworkRef
          }

        var alreadyPresent = false
        def raiseAlreadyPresent(): Unit =
          alreadyPresent = true
        var maybeRef1: Option[P2PNetworkRef[BftOrderingMessage]] = None
        var maybeRef2: Option[P2PNetworkRef[BftOrderingMessage]] = None

        checkP2PAddressState(isEndpointPresent = false, None)

        state.addNetworkRefIfMissing(addressId) { () =>
          raiseAlreadyPresent()
        } { () =>
          val ref = createNewNetworkRef()
          maybeRef1 = Some(ref)
          ref
        }

        alreadyPresent shouldBe false
        maybeRef1 should not be None
        checkP2PAddressState(isEndpointPresent = true, maybeRef1)

        state.addNetworkRefIfMissing(addressId) { () =>
          raiseAlreadyPresent()
        } { () =>
          val ref = createNewNetworkRef()
          maybeRef2 = Some(ref)
          ref
        }

        alreadyPresent shouldBe true
        maybeRef2 shouldBe None
        checkP2PAddressState(isEndpointPresent = true, maybeRef1)
      }
    }

    "consolidate network refs" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      val ref1 = createNewNetworkRef()
      state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

      state.connections should contain only None -> Some(APeerBftNodeId)

      val ref2 = createNewNetworkRef()
      state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref2)
      val ref3 = createNewNetworkRef()
      state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      verifyZeroInteractions(ref1)
      verify(ref2, times(1)).close()
      verifyZeroInteractions(ref3)

      state.connections should contain theSameElementsAs Seq(
        Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
        Some(AnotherPeerP2PEndpoint.id) -> None,
      )
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe true
    }

    "shutting down a connection" should {
      "remove the connection and close the network ref" in {
        Table[P2PAddress.Id, Map[P2PEndpoint.Id, BftNodeId], Boolean, Boolean](
          (
            "address ID",
            "endpoint associations",
            "clean network ref associations",
            "close network refs",
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            false,
            false,
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            true,
            false,
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            true,
            true,
          ),
          (
            APeerP2PEndpointAddressId,
            Map(APeerP2PEndpoint.id -> APeerBftNodeId),
            false,
            false,
          ),
          (
            APeerP2PEndpointAddressId,
            Map(APeerP2PEndpoint.id -> APeerBftNodeId),
            true,
            false,
          ),
          (APeerP2PEndpointAddressId, Map(APeerP2PEndpoint.id -> APeerBftNodeId), true, true),
          (AnotherPeerP2PEndpointAddressId, Map.empty, false, false),
          (AnotherPeerP2PEndpointAddressId, Map.empty, true, false),
          (AnotherPeerP2PEndpointAddressId, Map.empty, true, true),
        ).forEvery { (addressId, associations, clearNetworkRefAssociations, closeNetworkRefs) =>
          val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)
          associations.foreach { case (endpointId, bftNodeId) =>
            state.associateP2PEndpointIdToBftNodeId(endpointId, bftNodeId)
          }
          val ref = createNewNetworkRef()
          state.addNetworkRefIfMissing(addressId)(() => fail())(() => ref)
          addressId match {
            case Right(nodeId) =>
              state.addSenderIfMissing(nodeId, ASender).discard
            case Left(_) => ()
          }

          val result =
            state.shutdownConnectionAndReturnPeerSender(
              addressId,
              clearNetworkRefAssociations,
              closeNetworkRefs,
            )

          state.getSender(addressId) shouldBe None

          addressId match {
            case Right(nodeId) =>
              result should not be None

              state.getNetworkRef(nodeId) shouldBe (if (clearNetworkRefAssociations) None
                                                    else Some(ref))
            case Left(endpointId) =>
              result shouldBe None
              state.isDefined(endpointId) shouldBe !clearNetworkRefAssociations || associations
                .isDefinedAt(endpointId)
              state.isOutgoing(endpointId) shouldBe !clearNetworkRefAssociations && !associations
                .isDefinedAt(endpointId)
          }

          if (closeNetworkRefs)
            verify(ref, times(1)).close()
          else
            verifyZeroInteractions(ref)
        }
      }
    }

    "unassociating a sender" should {
      "remove the sender and return the endpoints associated with the node ID" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
        state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)

        state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true

        state.unassociateSenderAndReturnEndpointIds(ASender) should contain theSameElementsAs Seq(
          APeerP2PEndpoint.id,
          AnotherPeerP2PEndpoint.id,
        )
      }
    }
  }
}

object P2PGrpcConnectionStateTest {

  private val SelfBftNodeId = BftNodeId("1")

  private val APeerP2PEndpoint = PlainTextP2PEndpoint("1", Port.tryCreate(1))
  private val AnotherPeerP2PEndpoint = PlainTextP2PEndpoint("2", Port.tryCreate(2))

  private val APeerP2PEndpointAddressId = P2PAddress.Endpoint(APeerP2PEndpoint).id
  private val AnotherPeerP2PEndpointAddressId = P2PAddress.Endpoint(AnotherPeerP2PEndpoint).id

  private val APeerBftNodeId = BftNodeId("2")
  private val AnotherPeerBftNodeId = BftNodeId("3")
  private val APeerP2PNodeAddressId = P2PAddress.NodeId(APeerBftNodeId).id

  private val ASender = mock[StreamObserver[BftOrderingMessage]]
  private val AnotherSender = mock[StreamObserver[BftOrderingMessage]]

  private def createNewNetworkRef() = mock[P2PNetworkRef[BftOrderingMessage]]
}

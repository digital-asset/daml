// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerNetworkStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.memory.GenericInMemoryP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  endpointToTestBftNodeId,
  endpointToTestSequencerId,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail
import org.scalatest.wordspec.AnyWordSpec
import shapeless.*
import shapeless.HList.*
import shapeless.syntax.std.traversable.*

import java.time.Instant
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class P2PNetworkOutModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  import P2PNetworkOutModuleTest.*

  "p2p output" when {
    "ready" should {
      "connect to nodes and " +
        "initialize availability and " +
        "consensus once enough nodes are connected if starting from genesis" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val availabilitySpy =
            spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
          val consensusSpy =
            spy(fakeIgnoringModule[Consensus.Message[ProgrammableUnitTestEnv]])
          val outputSpy =
            spy(fakeIgnoringModule[Output.Message[ProgrammableUnitTestEnv]])
          val pruningSpy =
            spy(fakeIgnoringModule[Pruning.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithDefaultDepsExpectingSilence(
              mempool = mempoolSpy,
              availability = availabilitySpy,
              consensus = consensusSpy,
              output = outputSpy,
              pruning = pruningSpy,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          import state.*

          // No other node is authenticated
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 1
          mempoolStarted shouldBe true
          availabilityStarted shouldBe false
          consensusStarted shouldBe false
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(mempoolSpy, times(1)).asyncSend(Mempool.Start)
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
          verify(outputSpy, times(1)).asyncSend(Output.Start)
          verify(pruningSpy, times(1)).asyncSend(Pruning.Start)
          verify(availabilitySpy, never).asyncSend(
            any[Availability.Message[ProgrammableUnitTestEnv]]
          )(any[TraceContext], any[MetricsContext])
          verify(consensusSpy, never).asyncSend(any[Consensus.Message[ProgrammableUnitTestEnv]])(
            any[TraceContext],
            any[MetricsContext],
          )

          // One node authenticates -> weak quorum reached
          connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)

          context.selfMessages should contain theSameElementsInOrderAs
            Seq[P2PNetworkOut.Network](
              P2PNetworkOut.Network.Connected(otherInitialEndpointsTupled._1.id),
              P2PNetworkOut.Network
                .Authenticated(
                  endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
                  Some(otherInitialEndpointsTupled._1),
                ),
            )
          context.extractSelfMessages().foreach(module.receive)
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections.toMap
            .updated(
              Some(otherInitialEndpointsTupled._1.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 2
          mempoolStarted shouldBe true
          availabilityStarted shouldBe true
          consensusStarted shouldBe false
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, never).asyncSend(Consensus.Start)

          // One more nodes authenticated -> strong quorum reached
          connect(p2pNetworkManager, otherInitialEndpointsTupled._2)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._2)

          context.selfMessages should contain theSameElementsInOrderAs
            Seq[P2PNetworkOut.Network](
              P2PNetworkOut.Network.Connected(otherInitialEndpointsTupled._2.id),
              P2PNetworkOut.Network.Authenticated(
                endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
                Some(otherInitialEndpointsTupled._2),
              ),
            )
          context.extractSelfMessages().foreach(module.receive)
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 3))
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections.toMap
            .updated(
              Some(otherInitialEndpointsTupled._1.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
            .updated(
              Some(otherInitialEndpointsTupled._2.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._2)),
            )
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 3
          mempoolStarted shouldBe true
          availabilityStarted shouldBe true
          consensusStarted shouldBe true
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, times(1)).asyncSend(Consensus.Start)
        }

      "initialize availability and consensus immediately if NOT starting from genesis" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val availabilitySpy =
          spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val consensusSpy =
          spy(fakeIgnoringModule[Consensus.Message[ProgrammableUnitTestEnv]])
        val outputSpy =
          spy(fakeIgnoringModule[Output.Message[ProgrammableUnitTestEnv]])
        val pruningSpy =
          spy(fakeIgnoringModule[Pruning.Message])
        val (_, state, _, _) =
          setupWithDefaultDepsExpectingSilence(
            mempool = mempoolSpy,
            availability = availabilitySpy,
            consensus = consensusSpy,
            output = outputSpy,
            pruning = pruningSpy,
            isGenesis = false,
          )

        import state.*

        // No other node is authenticated
        p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections
        initialNodesConnecting shouldBe true
        maxNodesContemporarilyAuthenticated shouldBe 1
        mempoolStarted shouldBe true
        availabilityStarted shouldBe true
        consensusStarted shouldBe true
        outputStarted shouldBe true
        pruningStarted shouldBe true
        verify(mempoolSpy, times(1)).asyncSend(Mempool.Start)
        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
        verify(consensusSpy, times(1)).asyncSend(Consensus.Start)
        verify(outputSpy, times(1)).asyncSend(Output.Start)
        verify(pruningSpy, times(1)).asyncSend(Pruning.Start)
      }
    }

    "is requested to multicast a network message and " +
      "all nodes are authenticated" should {
        "send the message to all nodes" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)
          connect(p2pNetworkManager, otherInitialEndpointsTupled._2)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._2)
          context.extractSelfMessages().foreach(module.receive) // Authenticate all nodes

          val authenticatedEndpoints =
            Set(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2)
          val nodes = authenticatedEndpoints.map(endpointToTestBftNodeId)

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              nodes,
            )
          )

          authenticatedEndpoints.foreach(
            verify(sendActionSpy, times(1)).apply(
              _,
              BftOrderingMessage(
                "",
                Some(networkMessageBody),
                selfNode,
                None,
              ),
            )
          )
        }
      }

    "is requested to multicast a network message and " +
      "only some nodes are authenticated" should {
        "send the message only to authenticated nodes" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Authenticate all nodes

          val node = endpointToTestBftNodeId(otherInitialEndpointsTupled._1)

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              Set(node),
            )
          )

          val networkSend =
            BftOrderingMessage(
              "",
              Some(networkMessageBody),
              selfNode,
              None,
            )
          verify(sendActionSpy, times(1)).apply(
            otherInitialEndpointsTupled._1,
            networkSend,
          )
          verify(sendActionSpy, times(1)).apply(
            any[P2PEndpoint],
            any[BftOrderingMessage],
          )
        }
      }

    "is requested to multicast a network message to self" should {
      "forward it directly to the P2P network in module" in {
        val sendActionSpy =
          spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
        val p2pNetworkInSpy = spy(fakeIgnoringModule[BftOrderingMessage])
        val (context, _, module, _) = setupWithIgnoringDefaultDeps(sendActionSpy, p2pNetworkInSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
        module.receive(
          P2PNetworkOut.Multicast(
            P2PNetworkOut.BftOrderingNetworkMessage.Empty,
            Set(selfNode),
          )
        )

        verify(sendActionSpy, never).apply(
          any[P2PEndpoint],
          any[BftOrderingMessage],
        )
        verify(p2pNetworkInSpy, times(1)).asyncSend(
          BftOrderingMessage(
            traceContext = "",
            Some(networkMessageBody),
            selfNode,
            None,
          )
        )
      }
    }

    "it is requested to add a new endpoint" should {

      "add and connect the new endpoint" when {
        "the endpoint is not already stored" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val newEndpoint = anotherEndpoint

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              newEndpoint,
              added => endpointAdded = added,
            )
          )

          import state.*

          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          // Store and connect to endpoint
          context.runPipedMessagesThenVerifyAndReceiveOnModule(module) { message =>
            message shouldBe P2PNetworkOut.Internal.Connect(anotherEndpoint)
          }
          module.p2pEndpointsStore.listEndpoints
            .apply() should contain theSameElementsInOrderAs otherInitialEndpoints :+ anotherEndpoint

          endpointAdded shouldBe true
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections :+ Some(
            newEndpoint.id
          ) -> None

          authenticate(p2pNetworkManager, newEndpoint)
          context.extractSelfMessages().foreach(module.receive)

          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      // Check that we handle racing endpoint additions correctly
      "do nothing" when {
        "it is already stored but not yet connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val p2pEndpointsStore =
            new InMemoryUnitTestP2PEndpointsStore(otherInitialEndpoints.toSet)
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(
              mempool = mempoolSpy,
              p2pEndpointsStore = p2pEndpointsStore,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val newEndpoint = anotherEndpoint
          p2pEndpointsStore.addEndpoint(newEndpoint).apply() // Simulate a racing addition

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              newEndpoint,
              added => endpointAdded = added,
            )
          )

          context.runPipedMessages() shouldBe empty
          endpointAdded shouldBe false
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }
    }

    "it is requested to add an existing endpoint" should {
      "do nothing" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, state, module, _) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpointAdded = false
        module.receive(
          P2PNetworkOut.Admin.AddEndpoint(
            otherInitialEndpointsTupled._1,
            added => endpointAdded = added,
          )
        )

        context.runPipedMessages() shouldBe empty
        module.p2pEndpointsStore.listEndpoints
          .apply() should contain theSameElementsInOrderAs otherInitialEndpoints

        endpointAdded shouldBe false
        state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
      }
    }

    "it is requested to remove an existing endpoint" should {

      "remove and disconnect it" when {
        "it is stored" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          var endpointRemoved = false
          val removedEndpoint = otherInitialEndpointsTupled._1

          authenticate(p2pNetworkManager, removedEndpoint)

          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              removedEndpoint.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessagesAndReceiveOnModule(module) // Delete endpoint

          val remainingEndpoints =
            Seq(otherInitialEndpointsTupled._2, otherInitialEndpointsTupled._3)
          module.p2pEndpointsStore.listEndpoints
            .apply() should contain theSameElementsInOrderAs remainingEndpoints
          context.extractSelfMessages().foreach(module.receive) // Disconnect endpoint
          endpointRemoved shouldBe true

          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        }
      }

      // Check that we handle racing endpoint removals correctly
      "do nothing" when {
        "it is not stored anymore but still connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val p2pEndpointsStore = new InMemoryUnitTestP2PEndpointsStore(otherInitialEndpoints.toSet)
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(
              mempool = mempoolSpy,
              p2pEndpointsStore = p2pEndpointsStore,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          p2pEndpointsStore
            .removeEndpoint(otherInitialEndpointsTupled._1.id)
            .apply() // Simulate a racing removal

          var endpointRemoved = false
          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              otherInitialEndpointsTupled._1.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessages() shouldBe empty
          endpointRemoved shouldBe false
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }
    }

    "it is requested to remove a non-existing endpoint" should {
      "do nothing" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, state, module, _) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpointRemoved = false
        module.receive(
          P2PNetworkOut.Admin.RemoveEndpoint(
            anotherEndpoint.id,
            removed => endpointRemoved = removed,
          )
        )

        context.runPipedMessages() shouldBe empty
        module.p2pEndpointsStore.listEndpoints
          .apply() should contain theSameElementsInOrderAs otherInitialEndpoints

        import state.*

        endpointRemoved shouldBe false
        p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
      }
    }

    "it is queried about endpoints status" should {
      "return it" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, _, module, p2pNetworkManager) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        // Peer 1 is connected and authenticated
        connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
        authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)

        // Peer 2 is only connected
        connect(p2pNetworkManager, otherInitialEndpointsTupled._2)

        // Peer 3 is known but disconnected

        context.extractSelfMessages().foreach(module.receive) // Authenticate

        Table(
          "queried endpoint IDs" -> "expected status",
          Some(
            Seq(
              otherInitialEndpointsTupled._1.id,
              otherInitialEndpointsTupled._2.id,
              anotherEndpoint.id,
            )
          ) -> PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._1.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    endpointToTestSequencerId(otherInitialEndpointsTupled._1)
                  ),
                  None,
                ),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                anotherEndpoint.id,
                isOutgoingConnection = false,
                PeerEndpointHealth(PeerEndpointHealthStatus.UnknownEndpoint, None),
              ),
            )
          ),
          None -> PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._1.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    endpointToTestSequencerId(otherInitialEndpointsTupled._1)
                  ),
                  None,
                ),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._3.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None),
              ),
            )
          ),
        ) forEvery { (queriedEndpoints, expectedStatus) =>
          var status: Option[PeerNetworkStatus] = None
          module.receive(
            P2PNetworkOut.Admin.GetStatus(s => status = Some(s), queriedEndpoints)
          )
          status should contain(expectedStatus)
        }

        disconnect(p2pNetworkManager, otherInitialEndpointsTupled._2)
        context.extractSelfMessages().foreach(module.receive) // Process disconnection

        var status: Option[PeerNetworkStatus] = None
        module.receive(
          P2PNetworkOut.Admin
            .GetStatus(s => status = Some(s), Some(Seq(otherInitialEndpointsTupled._2.id)))
        )
        status should contain(
          PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None),
              )
            )
          )
        )

        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        verify(mempoolSpy, times(2)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
      }
    }

    "it is sent a topology update" should {
      "send an update to the mempool" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, _, module, _) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        module.receive(P2PNetworkOut.Network.TopologyUpdate(aMembership))

        verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
      }
    }
  }

  private def setupWithIgnoringDefaultDeps(
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit = (_, _) => (),
      p2pNetworkIn: ModuleRef[BftOrderingMessage] = fakeModuleExpectingSilence,
      mempool: ModuleRef[Mempool.Message] = fakeIgnoringModule,
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      pruning: ModuleRef[Pruning.Message] = fakeIgnoringModule,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestP2PEndpointsStore(
          otherInitialEndpoints.toSet
        ),
      isGenesis: Boolean = true,
  ): (
      ProgrammableUnitTestContext[P2PNetworkOut.Message],
      P2PNetworkOutModule.State,
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) =
    setupWithDefaultDepsExpectingSilence(
      mempool,
      availability,
      consensus,
      output,
      pruning,
      sendAction,
      p2pNetworkIn,
      p2pEndpointsStore,
      isGenesis,
    )

  private def setupWithDefaultDepsExpectingSilence(
      mempool: ModuleRef[Mempool.Message],
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]],
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]],
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]],
      pruning: ModuleRef[Pruning.Message],
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit = (_, _) => (),
      p2pNetworkIn: ModuleRef[BftOrderingMessage] = fakeModuleExpectingSilence,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestP2PEndpointsStore(
          otherInitialEndpoints.toSet
        ),
      isGenesis: Boolean = true,
  ): (
      ProgrammableUnitTestContext[P2PNetworkOut.Message],
      P2PNetworkOutModule.State,
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) = {
    val p2pConnectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
    val state =
      new P2PNetworkOutModule.State(
        p2pConnectionState,
        aMembership,
      )
    implicit val context: ProgrammableUnitTestContext[P2PNetworkOut.Message] =
      new ProgrammableUnitTestContext[P2PNetworkOut.Message](resolveAwaits = true)
    val (module, p2pNetworkManager) =
      createModule(
        sendAction,
        p2pNetworkIn,
        mempool,
        availability,
        consensus,
        output,
        pruning,
        state,
        p2pEndpointsStore,
        isGenesis,
      )
    module.ready(context.self)
    context.selfMessages should contain only P2PNetworkOut.Start
    context.extractSelfMessages().foreach(module.receive) // Start connecting to initial nodes
    (context, state, module, p2pNetworkManager)
  }

  private def createModule(
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit,
      p2pNetworkIn: ModuleRef[BftOrderingMessage],
      mempool: ModuleRef[Mempool.Message],
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]],
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]],
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]],
      pruning: ModuleRef[Pruning.Message],
      state: P2PNetworkOutModule.State,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv],
      isGenesis: Boolean,
  ): (
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) = {
    val dependencies = P2PNetworkOutModuleDependencies(
      (p2pConnectionEventListener, _) =>
        new FakeP2PNetworkManager(p2pConnectionEventListener, sendAction),
      p2pNetworkIn,
      mempool,
      availability,
      consensus,
      output,
      pruning,
    )
    val outputModule =
      new P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager](
        selfNode,
        isGenesis,
        state,
        p2pEndpointsStore,
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
        dependencies,
        P2PNetworkOutModuleTest.this.loggerFactory,
        P2PNetworkOutModuleTest.this.timeouts,
      )(MetricsContext.Empty)
    (outputModule, outputModule.p2pNetworkManager)
  }

  private def connect(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
  ): Unit =
    fakeClientP2PNetworkManager
      .nodeActions(endpoint)
      .onConnect(
        endpoint.id
      )

  private def disconnect(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
  ): Unit =
    fakeClientP2PNetworkManager
      .nodeActions(endpoint)
      .onDisconnect(
        endpoint.id
      )

  private def authenticate(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
      customNode: Option[BftNodeId] = None,
  ): Unit = {
    val bftNodeId = customNode.getOrElse(endpointToTestBftNodeId(endpoint))
    fakeClientP2PNetworkManager
      .nodeActions(endpoint)
      .onSequencerId(bftNodeId, Some(endpoint))
  }

  private class FakeP2PNetworkManager(
      p2pConnectionEventListener: P2PConnectionEventListener,
      asyncP2PSendAction: (P2PEndpoint, BftOrderingMessage) => Unit,
  ) extends P2PNetworkManager[ProgrammableUnitTestEnv, BftOrderingMessage]
      with NamedLogging {

    override val timeouts: ProcessingTimeout = P2PNetworkOutModuleTest.this.timeouts
    override val loggerFactory: NamedLoggerFactory = P2PNetworkOutModuleTest.this.loggerFactory

    val nodeActions: mutable.Map[P2PEndpoint, P2PConnectionEventListener] =
      mutable.Map.empty
    override def createNetworkRef[ActorContextT](
        context: ProgrammableUnitTestContext[ActorContextT],
        p2pAddress: P2PAddress,
    )(implicit traceContext: TraceContext): P2PNetworkRef[BftOrderingMessage] = {
      p2pAddress.maybeP2PEndpoint.foreach(nodeActions.put(_, p2pConnectionEventListener).discard)

      new P2PNetworkRef[BftOrderingMessage]() {
        override def asyncP2PSend(createMsg: Option[Instant] => BftOrderingMessage)(implicit
            traceContext: TraceContext,
            metricsContext: MetricsContext,
        ): Unit =
          p2pAddress.maybeP2PEndpoint.foreach(asyncP2PSendAction(_, createMsg(None)))

        override protected def timeouts: ProcessingTimeout =
          P2PNetworkOutModuleTest.this.timeouts

        override protected def logger: TracedLogger = P2PNetworkOutModuleTest.this.logger
      }
    }

    override def shutdownOutgoingConnection(
        p2pEndpointId: P2PEndpoint.Id
    )(implicit traceContext: TraceContext): Unit = ()
  }
}

object P2PNetworkOutModuleTest {

  final class InMemoryUnitTestP2PEndpointsStore(
      initialEndpoints: Set[P2PEndpoint]
  ) extends GenericInMemoryP2PEndpointsStore[ProgrammableUnitTestEnv](initialEndpoints) {
    override protected def createFuture[T](action: String)(
        value: () => Try[T]
    ): ProgrammableUnitTestEnv#FutureUnlessShutdownT[T] = () =>
      value() match {
        case Success(value) => value
        case Failure(exception) => fail(exception)
      }
    override def close(): Unit = ()
  }

  private lazy val selfNode: BftNodeId =
    endpointToTestBftNodeId(PlainTextP2PEndpoint(s"host0", Port.tryCreate(5000)))

  private lazy val otherInitialEndpoints =
    List(1, 2, 3)
      .map(i => PlainTextP2PEndpoint(s"host$i", Port.tryCreate(5000 + i)))

  private lazy val otherInitialEndpointIds =
    otherInitialEndpoints.map(_.id)

  private lazy val initialKnownConnections =
    otherInitialEndpointIds.map(Some(_) -> None)

  private lazy val anotherEndpoint =
    PlainTextP2PEndpoint("host4", Port.tryCreate(5004))

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private lazy val otherInitialEndpointsTupled =
    otherInitialEndpoints
      .toHList[P2PEndpoint :: P2PEndpoint :: P2PEndpoint :: HNil]
      .get
      .tupled

  private lazy val bftNodeIds = selfNode +: otherInitialEndpoints.map(endpointToTestBftNodeId)
  private lazy val aMembership =
    Membership(
      selfNode,
      OrderingTopology.forTesting(bftNodeIds.toSet),
      leaders = bftNodeIds,
    )
}

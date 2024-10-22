// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  BftOrderingMessageBody,
  BftOrderingServiceReceiveRequest,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.EnterpriseSequencerBftAdminData.{
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerEndpointStatus,
  PeerNetworkStatus,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.BftP2PNetworkOut
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.P2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.memory.GenericInMemoryP2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  ClientP2PNetworkManager,
  ModuleRef,
  P2PNetworkRef,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.BftP2PNetworkOutTest.InMemoryUnitTestP2pEndpointsStore
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail
import org.scalatest.wordspec.AnyWordSpec
import shapeless.*
import shapeless.HList.*
import shapeless.syntax.std.traversable.*

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class BftP2PNetworkOutTest extends AnyWordSpec with BftSequencerBaseTest {

  "p2p output" when {
    "ready" should {
      "connect to peers and " +
        "initialize availability and " +
        "consensus once enough peers are connected" in {
          val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
          val availabilitySpy =
            spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
          val consensusSpy = spy(fakeIgnoringModule[Consensus.Message[ProgrammableUnitTestEnv]])
          val (context, state, module) =
            setup(clientP2PNetworkManager, availability = availabilitySpy, consensus = consensusSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          import state.*

          // No other node is authenticated
          initialPeersConnecting shouldBe true
          knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints
          maxPeersContemporarilyAuthenticated shouldBe 0
          availabilityStarted shouldBe false
          consensusStarted shouldBe false
          verify(availabilitySpy, never).asyncSend(
            any[Availability.Message[ProgrammableUnitTestEnv]]
          )
          verify(consensusSpy, never).asyncSend(any[Consensus.Message[ProgrammableUnitTestEnv]])

          // One more node authenticates -> weak quorum reached
          authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._1)
          context.selfMessages should contain only P2PNetworkOut.Network
            .Authenticated(
              otherInitialEndpointsTupled._1,
              fakeSequencerId(otherInitialEndpointsTupled._1.toString),
            )
          context.extractSelfMessages().foreach(module.receive)
          initialPeersConnecting shouldBe true
          knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints
          maxPeersContemporarilyAuthenticated shouldBe 1
          availabilityStarted shouldBe true
          consensusStarted shouldBe false
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, never).asyncSend(Consensus.Start)

          // One more nodes authenticated -> strong quorum reached
          authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._2)
          context.selfMessages should contain only P2PNetworkOut.Network.Authenticated(
            otherInitialEndpointsTupled._2,
            fakeSequencerId(otherInitialEndpointsTupled._2.toString),
          )
          context.extractSelfMessages().foreach(module.receive)
          initialPeersConnecting shouldBe true
          knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints
          maxPeersContemporarilyAuthenticated shouldBe 2
          availabilityStarted shouldBe true
          consensusStarted shouldBe true
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, times(1)).asyncSend(Consensus.Start)
        }
    }

    "a peer tries to authenticate as self" should {
      "be disconnected" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val (context, state, module) = setup(clientP2PNetworkManager)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._1)
        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._2)
        suppressProblemLogs {
          authenticate(
            clientP2PNetworkManager,
            otherInitialEndpointsTupled._3,
            Some(selfSequencerId),
          )
          context.selfMessages.foreach(module.receive) // Perform all authentications
        }

        import state.*

        initialPeersConnecting shouldBe true
        knownPeers.getEndpoints should contain theSameElementsAs Seq(
          otherInitialEndpointsTupled._1,
          otherInitialEndpointsTupled._2,
        )
        maxPeersContemporarilyAuthenticated shouldBe 2
        availabilityStarted shouldBe true
        consensusStarted shouldBe true
      }
    }

    "a peer tries to re-authenticate as another peer" should {
      "be disconnected" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val (context, state, module) = setup(clientP2PNetworkManager)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._1)
        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._2)
        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._3)
        suppressProblemLogs {
          authenticate(
            clientP2PNetworkManager,
            otherInitialEndpointsTupled._3,
            Some(fakeSequencerId(otherInitialEndpointsTupled._2.toString)),
          )
          context.selfMessages.foreach(module.receive) // Perform all authentications
        }

        import state.*

        initialPeersConnecting shouldBe true
        knownPeers.getEndpoints should contain theSameElementsAs Seq(
          otherInitialEndpointsTupled._1,
          otherInitialEndpointsTupled._2,
        )
        maxPeersContemporarilyAuthenticated shouldBe 3
        availabilityStarted shouldBe true
        consensusStarted shouldBe true
      }
    }

    "is requested to multicast a network message and " +
      "all peers are authenticated" should {
        "send the message to all peers" in {
          val sendActionSpy = spyLambda((_: Endpoint, _: BftOrderingServiceReceiveRequest) => ())
          val clientP2PNetworkManager = new FakeClientP2PNetworkManager(sendActionSpy)
          val (context, _, module) = setup(clientP2PNetworkManager)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._1)
          authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._2)
          context.extractSelfMessages().foreach(module.receive) // Authenticate all peers

          val authenticatedEndpoints =
            Set(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2)
          val sequencerIds = authenticatedEndpoints.map(e => fakeSequencerId(e.toString))

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(networkMessageBody, signature = None, sequencerIds)
          )

          authenticatedEndpoints.foreach(
            verify(sendActionSpy, times(1)).apply(
              _,
              BftOrderingServiceReceiveRequest(
                "",
                Some(networkMessageBody),
                selfSequencerId.uid.toProtoPrimitive,
                signature = None,
              ),
            )
          )
        }
      }

    "is requested to multicast a network message and " +
      "only some peers are authenticated" should {
        "send the message only to authenticated peers" in {
          val sendActionSpy = spyLambda((_: Endpoint, _: BftOrderingServiceReceiveRequest) => ())
          val clientP2PNetworkManager = new FakeClientP2PNetworkManager(sendActionSpy)
          val (context, _, module) = setup(clientP2PNetworkManager)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2).foreach(
            authenticate(clientP2PNetworkManager, _)
          )
          context.extractSelfMessages().foreach(module.receive) // Authenticate all peers

          val sequencerId = fakeSequencerId(otherInitialEndpointsTupled._1.toString)

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(
              networkMessageBody,
              signature = None,
              Set(sequencerId),
            )
          )

          val networkSend = BftOrderingServiceReceiveRequest(
            "",
            Some(networkMessageBody),
            selfSequencerId.uid.toProtoPrimitive,
            signature = None,
          )
          verify(sendActionSpy, times(1)).apply(
            otherInitialEndpointsTupled._1,
            networkSend,
          )
          verify(sendActionSpy, times(1)).apply(
            any[Endpoint],
            any[BftOrderingServiceReceiveRequest],
          )
        }
      }

    "is requested to multicast a network message to self" should {
      "forward it directly to the P2P network in module" in {
        val sendActionSpy = spyLambda((_: Endpoint, _: BftOrderingServiceReceiveRequest) => ())
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager(sendActionSpy)
        val p2pNetworkInSpy = spy(fakeIgnoringModule[BftOrderingServiceReceiveRequest])
        val (context, _, module) = setup(clientP2PNetworkManager, p2pNetworkInSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
        module.receive(
          P2PNetworkOut.Multicast(
            networkMessageBody,
            None,
            Set(selfSequencerId),
          )
        )

        verify(sendActionSpy, never).apply(any[Endpoint], any[BftOrderingServiceReceiveRequest])
        verify(p2pNetworkInSpy, times(1)).asyncSend(
          BftOrderingServiceReceiveRequest(
            traceContext = "",
            Some(networkMessageBody),
            selfSequencerId.uid.toProtoPrimitive,
            signature = None,
          )
        )
      }
    }

    "it is requested to add a new endpoint" should {
      "add and connect the new endpoint" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val availabilitySpy = spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val (context, state, module) =
          setup(clientP2PNetworkManager, availability = availabilitySpy)

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
        knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints

        // Store and connect to endpoint
        context.runPipedMessagesThenVerifyAndReceiveOnModule(module) { message =>
          message shouldBe P2PNetworkOut.Internal.Connect(anotherEndpoint)
        }
        module.p2pEndpointsStore.listEndpoints
          .apply() should contain theSameElementsInOrderAs otherInitialEndpoints :+ anotherEndpoint

        endpointAdded shouldBe true
        knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints :+ newEndpoint

        authenticate(clientP2PNetworkManager, newEndpoint)
        context.extractSelfMessages().foreach(module.receive)

        maxPeersContemporarilyAuthenticated shouldBe 1
        availabilityStarted shouldBe true
        consensusStarted shouldBe false
        verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
      }
    }

    "it is requested to add an existing endpoint" should {
      "do nothing" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val availabilitySpy = spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val (context, state, module) =
          setup(clientP2PNetworkManager, availability = availabilitySpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpointAdded = false
        module.receive(
          P2PNetworkOut.Admin.AddEndpoint(
            otherInitialEndpointsTupled._1,
            added => endpointAdded = added,
          )
        )

        context.runPipedMessagesAndReceiveOnModule(module) // Store endpoint
        module.p2pEndpointsStore.listEndpoints
          .apply() should contain theSameElementsInOrderAs otherInitialEndpoints
        context.extractSelfMessages().foreach(module.receive) // Connect endpoint

        import state.*

        endpointAdded shouldBe false
        knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints
        maxPeersContemporarilyAuthenticated shouldBe 0
        availabilityStarted shouldBe false
        consensusStarted shouldBe false
        verify(availabilitySpy, never).asyncSend(any[Availability.Message[ProgrammableUnitTestEnv]])
      }
    }

    "it is requested to remove an existing endpoint" should {
      "disconnect it" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val availabilitySpy = spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val (context, state, module) =
          setup(clientP2PNetworkManager, availability = availabilitySpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpointRemoved = false
        module.receive(
          P2PNetworkOut.Admin.RemoveEndpoint(
            otherInitialEndpointsTupled._1,
            removed => endpointRemoved = removed,
          )
        )

        context.runPipedMessagesAndReceiveOnModule(module) // Delete endpoint
        module.p2pEndpointsStore.listEndpoints.apply() should contain theSameElementsInOrderAs Seq(
          otherInitialEndpointsTupled._2,
          otherInitialEndpointsTupled._3,
        )
        context.extractSelfMessages().foreach(module.receive) // Disconnect endpoint

        import state.*

        endpointRemoved shouldBe true
        knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints.tail
      }
    }

    "it is requested to remove a non-existing endpoint" should {
      "do nothing" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val availabilitySpy = spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val (context, state, module) =
          setup(clientP2PNetworkManager, availability = availabilitySpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpointRemoved = false
        module.receive(
          P2PNetworkOut.Admin.RemoveEndpoint(
            anotherEndpoint,
            removed => endpointRemoved = removed,
          )
        )

        // Delete endpoint (shouldn't have any effect)
        context.runPipedMessagesAndReceiveOnModule(module)
        module.p2pEndpointsStore.listEndpoints
          .apply() should contain theSameElementsInOrderAs otherInitialEndpoints
        // Disconnect endpoint (shouldn't have any effect)
        context.extractSelfMessages().foreach(module.receive)

        import state.*

        endpointRemoved shouldBe false
        knownPeers.getEndpoints should contain theSameElementsAs otherInitialEndpoints
      }
    }

    "it is queried about endpoints status" should {
      "return it" in {
        val clientP2PNetworkManager = new FakeClientP2PNetworkManager()
        val (context, _, module) = setup(clientP2PNetworkManager)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        authenticate(clientP2PNetworkManager, otherInitialEndpointsTupled._1)
        context.extractSelfMessages().foreach(module.receive) // Authenticate

        Table(
          "queried endpoints" -> "expected status",
          Some(
            Seq(
              otherInitialEndpointsTupled._1,
              otherInitialEndpointsTupled._2,
              anotherEndpoint,
            )
          ) -> PeerNetworkStatus(
            Seq(
              PeerEndpointStatus(
                otherInitialEndpointsTupled._1,
                PeerEndpointHealth(PeerEndpointHealthStatus.Authenticated, None),
              ),
              PeerEndpointStatus(
                otherInitialEndpointsTupled._2,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerEndpointStatus(
                anotherEndpoint,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unknown, None),
              ),
            )
          ),
          None -> PeerNetworkStatus(
            Seq(
              PeerEndpointStatus(
                otherInitialEndpointsTupled._1,
                PeerEndpointHealth(PeerEndpointHealthStatus.Authenticated, None),
              ),
              PeerEndpointStatus(
                otherInitialEndpointsTupled._2,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerEndpointStatus(
                otherInitialEndpointsTupled._3,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
            )
          ),
        ) forEvery { (queriedEndpoints, expectedStatus) =>
          var status: Option[PeerNetworkStatus] = None
          module.receive(
            P2PNetworkOut.Admin.GetStatus(queriedEndpoints, s => status = Some(s))
          )
          status should contain(expectedStatus)
        }
      }
    }
  }

  private def setup(
      clientP2PNetworkManager: FakeClientP2PNetworkManager,
      p2pNetworkIn: ModuleRef[BftOrderingServiceReceiveRequest] = fakeIgnoringModule,
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
  ): (
      ProgrammableUnitTestContext[P2PNetworkOut.Message],
      BftP2PNetworkOut.State,
      BftP2PNetworkOut[ProgrammableUnitTestEnv],
  ) = {
    val state = new BftP2PNetworkOut.State()
    implicit val context: ProgrammableUnitTestContext[P2PNetworkOut.Message] =
      new ProgrammableUnitTestContext[P2PNetworkOut.Message](resolveAwaits = true)
    val module = createModule(
      clientP2PNetworkManager = clientP2PNetworkManager,
      p2pNetworkIn = p2pNetworkIn,
      availability = availability,
      consensus = consensus,
      output = output,
      state = state,
    )
    module.ready(context.self)
    context.selfMessages should contain only P2PNetworkOut.Start
    context.extractSelfMessages().foreach(module.receive)
    (context, state, module)
  }

  private def createModule(
      clientP2PNetworkManager: ClientP2PNetworkManager[
        ProgrammableUnitTestEnv,
        BftOrderingServiceReceiveRequest,
      ],
      p2pEndpointsStore: P2pEndpointsStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestP2pEndpointsStore(
          otherInitialEndpoints.toSet
        ),
      p2pNetworkIn: ModuleRef[BftOrderingServiceReceiveRequest] = fakeIgnoringModule,
      mempool: ModuleRef[Mempool.Message] = fakeIgnoringModule,
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      state: BftP2PNetworkOut.State = new BftP2PNetworkOut.State(),
  ): BftP2PNetworkOut[ProgrammableUnitTestEnv] = {
    val dependencies = P2PNetworkOutModuleDependencies(
      clientP2PNetworkManager,
      p2pNetworkIn,
      mempool,
      availability,
      consensus,
      output,
    )
    new BftP2PNetworkOut[ProgrammableUnitTestEnv](
      selfSequencerId,
      p2pEndpointsStore,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      dependencies,
      BftP2PNetworkOutTest.this.loggerFactory,
      BftP2PNetworkOutTest.this.timeouts,
      state,
    )(MetricsContext.Empty)
  }

  private def authenticate(
      fakeClientP2PNetworkManager: FakeClientP2PNetworkManager,
      endpoint: Endpoint,
      customSequencerId: Option[SequencerId] = None,
  ): Unit =
    fakeClientP2PNetworkManager.onSequencerIdActions(endpoint)(
      endpoint,
      customSequencerId.getOrElse(fakeSequencerId(endpoint.toString)),
    )

  private class FakeClientP2PNetworkManager(
      asyncP2PSendAction: (Endpoint, BftOrderingServiceReceiveRequest) => Unit = (_, _) => ()
  ) extends ClientP2PNetworkManager[ProgrammableUnitTestEnv, BftOrderingServiceReceiveRequest] {

    val onSequencerIdActions: mutable.Map[Endpoint, (Endpoint, SequencerId) => Unit] =
      mutable.Map.empty

    override def createNetworkRef[ActorContextT](
        context: ProgrammableUnitTestContext[ActorContextT],
        peer: Endpoint,
    )(
        onSequencerId: (Endpoint, SequencerId) => Unit
    ): P2PNetworkRef[BftOrderingServiceReceiveRequest] = {
      onSequencerIdActions.put(peer, onSequencerId)

      new P2PNetworkRef[BftOrderingServiceReceiveRequest]() {
        override def asyncP2PSend(msg: BftOrderingServiceReceiveRequest)(
            onCompletion: => Unit
        )(implicit
            traceContext: TraceContext
        ): Unit = {
          asyncP2PSendAction(peer, msg)
          onCompletion
        }

        override protected def timeouts: ProcessingTimeout =
          BftP2PNetworkOutTest.this.timeouts

        override protected def logger: TracedLogger = BftP2PNetworkOutTest.this.logger
      }
    }
  }

  private lazy val selfSequencerId: SequencerId = fakeSequencerId("")

  private lazy val otherInitialEndpoints =
    List(1, 2, 3)
      .map(i => new Endpoint(s"host$i", Port.tryCreate(5000 + i)))

  private lazy val anotherEndpoint = new Endpoint("host4", Port.tryCreate(5004))

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private lazy val otherInitialEndpointsTupled =
    otherInitialEndpoints.toHList[Endpoint :: Endpoint :: Endpoint :: HNil].get.tupled
}

object BftP2PNetworkOutTest {

  final class InMemoryUnitTestP2pEndpointsStore(
      initialEndpoints: Set[Endpoint]
  ) extends GenericInMemoryP2pEndpointsStore[ProgrammableUnitTestEnv](initialEndpoints) {
    override protected def createFuture[T](action: String)(
        value: () => Try[T]
    ): ProgrammableUnitTestEnv#FutureUnlessShutdownT[T] = () =>
      value() match {
        case Success(value) => value
        case Failure(exception) => fail(exception)
      }
  }
}

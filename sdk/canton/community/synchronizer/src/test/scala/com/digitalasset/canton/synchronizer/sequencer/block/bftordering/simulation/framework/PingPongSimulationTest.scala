// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.framework

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.endpointToTestBftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
  Pruning,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.EmptyOnboardingManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleName,
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration.DurationInt

class Recorder {
  var pingActorReceivedClientPing = false
  var pingActorReceivedClockPing = false
  var pingHelperActorStopped = false
  var pingActorStopped = false
  var pongActorStopped = false
}

final case class State(clientPing: Boolean = false, clockPing: Boolean = false)

final case class PingHelper[E <: Env[E]](
    ping: ModuleRef[String],
    recorder: Recorder,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, String] {
  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override protected def receiveInternal(
      message: String
  )(implicit context: E#ActorContextT[String], traceContext: TraceContext): Unit = message match {
    case "tick" =>
      ping.asyncSend("tick-ack")
      context.stop()
      recorder.pingHelperActorStopped = true
    case _ => sys.error(s"Unexpected message: $message")
  }
}

final case class Ping[E <: Env[E]](
    otherNode: P2PNetworkRef[String],
    recorder: Recorder,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    state: State = State(),
) extends Module[E, String] {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override def ready(self: ModuleRef[String]): Unit =
    ifCompleteNotifyNodes(self)(TraceContext.empty)

  override def receiveInternal(message: String)(implicit
      context: E#ActorContextT[String],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case "init" =>
        context.delayedEvent(5.seconds, "tick")
        otherNode.asyncP2PSend(_ => "ping")
      case "tick" =>
        val helperRef = context.newModuleRef[String](ModuleName("ping-helper"))()
        val helper = PingHelper[E](context.self, recorder, loggerFactory, timeouts)
        context.setModule(helperRef, helper)
        helper.ready(helperRef)
        helperRef.asyncSend("tick")
      case "tick-ack" =>
        otherNode.asyncP2PSend(_ => "ping-tick")
      case "pong" =>
        recorder.pingActorReceivedClientPing = true
        context.become(copy[E](state = state.copy(clientPing = true)))
      case "pong-tick" =>
        recorder.pingActorReceivedClockPing = true
        context.become(copy[E](state = state.copy(clockPing = true)))
      case "complete" =>
        recorder.pingActorStopped = true
        context.stop()
      case _ => sys.error(s"Unexpected message: $message")
    }

  private def ifCompleteNotifyNodes(
      self: ModuleRef[String]
  )(implicit traceContext: TraceContext): Unit =
    if (state.clientPing && state.clockPing) {
      otherNode.asyncP2PSend(_ => "complete")
      self.asyncSend("complete")
    }
}

final case class Pong[E <: Env[E]](
    otherNode: P2PNetworkRef[String],
    recorder: Recorder,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, String] {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override def receiveInternal(message: String)(implicit
      context: E#ActorContextT[String],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case "ping" =>
        otherNode.asyncP2PSend(_ => "pong")
      case "ping-tick" =>
        otherNode.asyncP2PSend(_ => "pong-tick")
      case "complete" =>
        recorder.pongActorStopped = true
        context.stop()
      case _ => sys.error(s"Unexpected message: $message")
    }
}

final case class PingerClient[E <: Env[E]](
    pinger: ModuleRef[String],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, Unit] {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override def receiveInternal(message: Unit)(implicit
      context: E#ActorContextT[Unit],
      traceContext: TraceContext,
  ): Unit =
    pinger.asyncSend("init")
}

object TestSystem {

  def mkPinger[
      E <: Env[E],
      P2PNetworkManagerT <: P2PNetworkManager[E, String],
  ](
      pongerEndpoint: P2PEndpoint,
      recorder: Recorder,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SystemInitializer[E, P2PNetworkManagerT, String, String] =
    (system, createP2PNetworkManager) => {
      val inputModuleRef = system.newModuleRef[String](ModuleName("ping"))()
      val p2pNetworkManager =
        createP2PNetworkManager(P2PConnectionEventListener.NoOp, inputModuleRef)
      val pongerRef = p2pNetworkManager.createNetworkRef(
        system.rootActorContext,
        P2PAddress.Endpoint(pongerEndpoint),
      )(TraceContext.empty)
      val module = Ping[E](pongerRef, recorder, loggerFactory, timeouts)
      system.setModule[String](inputModuleRef, module)
      val p2PAdminModuleRef =
        system.newModuleRef[P2PNetworkOut.Admin](ModuleName("p2PAdminModule"))()
      val consensusAdminModuleRef =
        system.newModuleRef[Consensus.Admin](ModuleName("consensusAdminModule"))()
      val outputModuleRef =
        system.newModuleRef[Output.SequencerSnapshotMessage](ModuleName("outputModule"))()
      val pruningModuleRef =
        system.newModuleRef[Pruning.Message](ModuleName("pruningModule"))()
      SystemInitializationResult(
        inputModuleRef,
        inputModuleRef,
        p2PAdminModuleRef,
        consensusAdminModuleRef,
        outputModuleRef,
        pruningModuleRef,
        p2pNetworkManager,
      )
    }

  def mkPonger[
      E <: Env[E],
      P2PNetworkManagerT <: P2PNetworkManager[E, String],
  ](
      pingerEndpoint: P2PEndpoint,
      recorder: Recorder,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SystemInitializer[E, P2PNetworkManagerT, String, String] =
    (system, createP2PNetworkManager) => {
      val inputModuleRef = system.newModuleRef[String](ModuleName("pong"))()
      val p2pNetworkManager =
        createP2PNetworkManager(P2PConnectionEventListener.NoOp, inputModuleRef)
      val pingerRef = p2pNetworkManager.createNetworkRef(
        system.rootActorContext,
        P2PAddress.Endpoint(pingerEndpoint),
      )(TraceContext.empty)
      val module = Pong[E](pingerRef, recorder, loggerFactory, timeouts)
      system.setModule[String](inputModuleRef, module)
      val p2PAdminModuleRef =
        system.newModuleRef[P2PNetworkOut.Admin](ModuleName("p2PAdminModule"))()
      val consensusAdminModuleRef =
        system.newModuleRef[Consensus.Admin](ModuleName("consensusAdminModule"))()
      val outputModuleRef =
        system.newModuleRef[Output.SequencerSnapshotMessage](ModuleName("outputModule"))()
      val pruningModuleRef =
        system.newModuleRef[Pruning.Message](ModuleName("pruningModule"))()
      SystemInitializationResult(
        inputModuleRef,
        inputModuleRef,
        p2PAdminModuleRef,
        consensusAdminModuleRef,
        outputModuleRef,
        pruningModuleRef,
        p2pNetworkManager,
      )
    }

  def pingerClient[E <: Env[E]](
      loggerFactory: NamedLoggerFactory,
      timeout: ProcessingTimeout,
  ): SimulationClient.Initializer[E, Unit, String] =
    new SimulationClient.Initializer[E, Unit, String] {
      private implicit val metricsContext: MetricsContext = MetricsContext.Empty

      override def createClient(systemRef: ModuleRef[String]): Module[E, Unit] =
        PingerClient(systemRef, loggerFactory, timeout)

      override def init(context: E#ActorContextT[Unit]): Unit =
        context.delayedEventNoTrace(0.seconds, ())
    }
}

class PingPongSimulationTest extends AnyFlatSpec with BaseTest {

  it should "simple test" in {
    val simSettings = SimulationSettings(
      localSettings = LocalSettings(randomSeed = 4),
      networkSettings = NetworkSettings(randomSeed = 4),
      durationOfFirstPhaseWithFaults = 2.minutes,
    )

    val fakePort = Port.tryCreate(0)
    val pingerEndpoint = PlainTextP2PEndpoint("pinger", fakePort).asInstanceOf[P2PEndpoint]
    val pongerEndpoint = PlainTextP2PEndpoint("ponger", fakePort).asInstanceOf[P2PEndpoint]
    val recorder = new Recorder

    val topologyInit = Map(
      pingerEndpoint -> SimulationInitializer[Unit, String, String, Unit](
        (_: Unit) => TestSystem.mkPinger(pongerEndpoint, recorder, loggerFactory, timeouts),
        TestSystem.pingerClient(loggerFactory, timeouts),
        new P2PGrpcConnectionState(endpointToTestBftNodeId(pingerEndpoint), loggerFactory),
      ),
      pongerEndpoint -> SimulationInitializer
        .noClient[String, String, Unit](
          loggerFactory,
          timeouts,
        )(
          TestSystem.mkPonger(pingerEndpoint, recorder, loggerFactory, timeouts),
          new P2PGrpcConnectionState(endpointToTestBftNodeId(pongerEndpoint), loggerFactory),
        ),
    )
    val simulation =
      SimulationModuleSystem(
        topologyInit,
        EmptyOnboardingManager,
        simSettings,
        new SimClock(loggerFactory = loggerFactory),
        timeouts,
        loggerFactory,
      )

    val _ = simulation.run()

    assert(recorder.pingActorReceivedClientPing)
    assert(recorder.pingActorReceivedClockPing)
    assert(recorder.pingHelperActorStopped)
    assert(recorder.pingActorStopped)
    assert(recorder.pongActorStopped)
  }
}

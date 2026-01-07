// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.framework

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.EmptyOnboardingManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleName,
  P2PConnectionEventListener,
  P2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.framework.PipeTest.{
  Reporter,
  SimulatedPipeStore,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object PipeTest {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  trait PipeStore[E <: Env[E]] {
    def load(x: Int): E#FutureUnlessShutdownT[String]
  }

  class SimulatedPipeStore extends PipeStore[SimulationEnv] {
    override def load(x: Int): SimulationFuture[String] =
      SimulationFuture("load")(() => Success(x.toString))
  }

  class Reporter {
    var `got response` = false
    var `got zip response` = false
    var `got sequence response` = false
  }

  class PipeNode[E <: Env[E]](
      pipeStore: PipeStore[E],
      reporter: Reporter,
      override val loggerFactory: NamedLoggerFactory,
      override val timeouts: ProcessingTimeout,
  ) extends Module[E, String] {

    /** The module's message handler.
      *
      * @param context
      *   Environment-specific information, such as the representation of the actor's state.
      */
    override def receiveInternal(message: String)(implicit
        context: E#ActorContextT[String],
        traceContext: TraceContext,
    ): Unit = message match {
      case "init" =>
        pipeToSelf(pipeStore.load(5)) {
          case Failure(_) =>
            abort("something went wrong")
          case Success(value) => s"single-future($value)"
        }
        val future1 = context.zipFuture(pipeStore.load(10), pipeStore.load(20))
        pipeToSelf(future1) {
          case Failure(exception) =>
            abort("Something went wrong", exception)
          case Success((val1, val2)) => s"zip($val1,$val2)"
        }
        val future2 = context.sequenceFuture(Seq(0, 1, 2).map(pipeStore.load(_)))
        pipeToSelf(future2) {
          case Failure(exception) =>
            abort("Something went wrong", exception)
          case Success(xs) => s"sequence(${xs.mkString(",")})"
        }

      case "single-future(5)" =>
        reporter.`got response` = true

      case "zip(10,20)" =>
        reporter.`got zip response` = true

      case "sequence(0,1,2)" =>
        reporter.`got sequence response` = true

      case other =>
        abort(s"Can't handle message: $other")
    }
  }

  def mkNode[
      E <: Env[E],
      P2PNetworkManagerT
        <: P2PNetworkManager[
          E,
          String,
        ],
  ](
      pipeStore: PipeStore[E],
      reporter: Reporter,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SystemInitializer[E, P2PNetworkManagerT, String, String] =
    (system, createP2PNetworkManager) => {
      val inputModuleRef = system.newModuleRef[String](ModuleName("module"))()
      val p2pNetworkManager =
        createP2PNetworkManager(P2PConnectionEventListener.NoOp, inputModuleRef)
      val module = new PipeNode[E](pipeStore, reporter, loggerFactory, timeouts)
      system.setModule(inputModuleRef, module)
      val p2PAdminModuleRef =
        system.newModuleRef[P2PNetworkOut.Admin](ModuleName("p2PAdminModule"))()
      val consensusAdminModuleRef =
        system.newModuleRef[Consensus.Admin](ModuleName("consensusAdminModule"))()
      val outputModuleRef =
        system.newModuleRef[Output.Message[E]](ModuleName("outputModule"))()
      val pruningModuleRef =
        system.newModuleRef[Pruning.Message](ModuleName("pruningModule"))()
      inputModuleRef.asyncSendNoTrace("init")
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
}

class PipeTest extends AnyFlatSpec with BaseTest {

  it should "simulation should implement pipeToSelf correctly" in {
    val simSettings = SimulationSettings(
      localSettings = LocalSettings(randomSeed = 4),
      networkSettings = NetworkSettings(randomSeed = 4),
      durationOfFirstPhaseWithFaults = 2.minutes,
    )

    val theEndpoint = PlainTextP2PEndpoint("node", Port.tryCreate(0)).asInstanceOf[P2PEndpoint]
    val reporter = new Reporter
    val pipeStore = new SimulatedPipeStore

    val topologyInit = Map(
      theEndpoint -> SimulationInitializer
        .noClient[String, String, Unit](
          loggerFactory,
          timeouts,
        )(
          PipeTest.mkNode(pipeStore, reporter, loggerFactory, timeouts),
          new P2PGrpcConnectionState(endpointToTestBftNodeId(theEndpoint), loggerFactory),
        )
    )

    val simulation = SimulationModuleSystem(
      topologyInit,
      EmptyOnboardingManager,
      simSettings,
      new SimClock(loggerFactory = loggerFactory),
      timeouts,
      loggerFactory,
    )

    simulation.run().discard

    reporter.`got response` shouldBe true
    reporter.`got zip response` shouldBe true
    reporter.`got sequence response` shouldBe true
  }
}

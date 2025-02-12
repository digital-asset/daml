// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import cats.Traverse
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  ModuleControl,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingDataProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  ClientP2PNetworkManager,
  Env,
  Module,
  ModuleContext,
  ModuleName,
  ModuleRef,
  ModuleSystem,
  P2PNetworkRef,
  PureFun,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import SimulationModuleSystem.SimulationP2PNetworkManager.fakeSequencerId

object SimulationModuleSystem {

  private[simulation] final case class SimulationModuleRef[-MessageT](
      name: ModuleName,
      collector: NodeCollector,
  ) extends ModuleRef[MessageT] {

    override def asyncSend(msg: MessageT): Unit =
      collector.addInternalEvent(name, ModuleControl.Send(msg))
  }

  private[simulation] final case class SimulationP2PNetworkRef[P2PMessageT](
      peer: SequencerId,
      collector: NodeCollector,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends P2PNetworkRef[P2PMessageT]
      with NamedLogging {

    override def asyncP2PSend(msg: P2PMessageT)(onCompletion: => Unit)(implicit
        traceContext: TraceContext
    ): Unit = {
      collector.addNetworkEvent(peer, msg)
      onCompletion
    }
  }

  private[simulation] final case class SimulationP2PNetworkManager[P2PMessageT](
      collector: NodeCollector,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ClientP2PNetworkManager[SimulationEnv, P2PMessageT]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        _context: SimulationModuleContext[ActorContextT],
        endpoint: Endpoint,
    )(
        onSequencerId: (Endpoint, SequencerId) => Unit
    ): P2PNetworkRef[P2PMessageT] = {
      val sequencerId = fakeSequencerId(endpoint)
      collector.addOpenConnection(sequencerId, endpoint, onSequencerId)
      SimulationP2PNetworkRef(sequencerId, collector, timeouts, loggerFactory)
    }
  }

  object SimulationP2PNetworkManager {
    def fakeSequencerId(peer: Endpoint): SequencerId =
      SequencerId(UniqueIdentifier.tryCreate("ns", s"fake_$peer"))
  }

  private[simulation] trait SimulationModuleContext[MessageT]
      extends ModuleContext[SimulationEnv, MessageT] {

    // Metrics are not produced in simulation tests
    override def timeFuture[X](timer: Timer, futureUnlessShutdown: => SimulationFuture[X])(implicit
        mc: MetricsContext
    ): SimulationFuture[X] = futureUnlessShutdown

    override def zipFuture[X, Y](
        future1: SimulationFuture[X],
        future2: SimulationFuture[Y],
    ): SimulationFuture[(X, Y)] =
      SimulationFuture.Zip(future1, future2)

    override def sequenceFuture[A, F[_]](futures: F[SimulationFuture[A]])(implicit
        ev: Traverse[F]
    ): SimulationFuture[F[A]] =
      SimulationFuture.Sequence(futures)

    override def mapFuture[X, Y](future: SimulationFuture[X])(
        fun: PureFun[X, Y]
    ): SimulationFuture[Y] = SimulationFuture.Map(future, fun)

    override def pureFuture[X](x: X): SimulationFuture[X] =
      SimulationFuture.Pure(s"pure($x)", () => Try(x))

    override def flatMapFuture[R1, R2](
        future1: SimulationFuture[R1],
        future2: PureFun[R1, SimulationFuture[R2]],
    ): SimulationFuture[R2] =
      SimulationFuture.FlatMap(future1, future2)

    override def abort(): Nothing =
      fail("Simulation failed in call to abort")

    override def abort(msg: String): Nothing =
      fail(s"Simulation failed in call to abort: $msg")

    override def abort(failure: Throwable): Nothing =
      fail(failure)

    override def blockingAwait[X](future: SimulationFuture[X]): X = future
      .resolveValue()
      .fold(abort(_), identity)

    override def blockingAwait[X](future: SimulationFuture[X], duration: FiniteDuration): X = future
      .resolveValue()
      .fold(abort(_), identity)
  }

  private final case class SimulationCancelable[E](collector: Collector[E], tickId: Int)
      extends CancellableEvent {
    override def cancel(): Boolean = {
      collector.addCancelTick(tickId)
      true
    }
  }

  private[simulation] final case class SimulationModuleNodeContext[MessageT](
      to: ModuleName,
      collector: NodeCollector,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override val self: SimulationModuleRef[MessageT] = SimulationModuleRef(to, collector)

    override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent = {
      val tickCounter = collector.addTickEvent(delay, to, ModuleControl.Send(message))
      SimulationCancelable(collector, tickCounter)
    }

    override def pipeToSelfInternal[X](futureUnlessShutdown: SimulationFuture[X])(
        fun: Try[X] => Option[MessageT]
    ): Unit = collector.addFuture(to, futureUnlessShutdown, fun)

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    ): SimulationModuleRef[NewModuleMessageT] =
      SimulationModuleRef(moduleName, collector)

    override def setModule[NewModuleMessageT](
        moduleRef: SimulationModuleRef[NewModuleMessageT],
        module: Module[SimulationEnv, NewModuleMessageT],
    ): Unit =
      addSetBehaviorEvent(collector, moduleRef.name, module, ready = false)

    override def become(module: Module[SimulationEnv, MessageT]): Unit =
      addSetBehaviorEvent(collector, to, module, ready = true)

    override def stop(onStop: () => Unit): Unit =
      collector.addInternalEvent(to, ModuleControl.Stop(onStop))

  }

  private[simulation] final case class SimulationModuleClientContext[MessageT](
      collector: ClientCollector,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    ): SimulationModuleRef[NewModuleMessageT] =
      unsupportedForClientModules()

    override def setModule[NewModuleMessageT](
        moduleRef: SimulationModuleRef[NewModuleMessageT],
        module: Module[SimulationEnv, NewModuleMessageT],
    ): Unit =
      unsupportedForClientModules()

    override def self: SimulationModuleRef[MessageT] = unsupportedForClientModules()

    override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent = {
      val tickId = collector.addTickEvent(delay, message)
      SimulationCancelable(collector, tickId)
    }

    override def pipeToSelfInternal[X](futureUnlessShutdown: SimulationFuture[X])(
        fun: Try[X] => Option[MessageT]
    ): Unit = unsupportedForClientModules()

    override def become(module: Module[SimulationEnv, MessageT]): Unit =
      unsupportedForClientModules()

    override def stop(onStop: () => Unit): Unit =
      unsupportedForClientModules()

    private def unsupportedForClientModules(): Nothing =
      sys.error("Unsupported for client modules")
  }

  private[simulation] final class SimulationModuleSystemContext[MessageT](
      collector: NodeCollector,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    ): SimulationModuleRef[NewModuleMessageT] =
      SimulationModuleRef(moduleName, collector)

    override def setModule[NewModuleMessageT](
        moduleRef: SimulationModuleRef[NewModuleMessageT],
        module: Module[SimulationEnv, NewModuleMessageT],
    ): Unit =
      addSetBehaviorEvent(collector, moduleRef.name, module, ready = false)

    override def self: SimulationModuleRef[MessageT] = unsupportedForSystem()

    override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent =
      unsupportedForSystem()

    override def pipeToSelfInternal[X](futureUnlessShutdown: SimulationFuture[X])(
        fun: Try[X] => Option[MessageT]
    ): Unit = unsupportedForSystem()

    private def unsupportedForSystem(): Nothing =
      sys.error("Unsupported for system object")

    override def become(module: Module[SimulationEnv, MessageT]): Unit = unsupportedForSystem()

    override def stop(onStop: () => Unit): Unit = unsupportedForSystem()

  }

  final class SimulationEnv extends Env[SimulationEnv] {
    override type ActorContextT[MessageT] = SimulationModuleContext[MessageT]
    override type ModuleRefT[-MessageT] = SimulationModuleRef[MessageT]
    override type FutureUnlessShutdownT[MessageT] = SimulationFuture[MessageT]
  }

  private[simulation] final class SimulationModuleSystem(
      collector: NodeCollector,
      val loggerFactory: NamedLoggerFactory,
  ) extends ModuleSystem[SimulationEnv] {

    override def rootActorContext: SimulationModuleContext[?] =
      new SimulationModuleSystemContext(collector, loggerFactory)

    override def newModuleRef[MessageT](
        moduleName: ModuleName // Must be unique per ref, else it will crash on spawn
    ): SimulationModuleRef[MessageT] =
      SimulationModuleRef(moduleName, collector)

    override def setModule[MessageT](
        moduleRef: SimulationModuleRef[MessageT],
        module: Module[SimulationEnv, MessageT],
    ): Unit =
      addSetBehaviorEvent(collector, moduleRef.name, module, ready = false)
  }

  private final case class SimulatedRefForClient[MessageT](collector: ClientCollector)
      extends ModuleRef[MessageT] {
    override def asyncSend(msg: MessageT): Unit = collector.addClientRequest(msg)
  }

  /** A simulation initializer comprises initializers for both the system and the client.
    */
  final case class SimulationInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
  ](
      systemInitializerFactory: OnboardingDataT => SystemInitializer[
        SimulationEnv,
        SystemNetworkMessageT,
        SystemInputMessageT,
      ],
      clientInitializer: SimulationClient.Initializer[
        SimulationEnv,
        ClientMessageT,
        SystemInputMessageT,
      ],
      initializeImmediately: Boolean = true,
  )

  object SimulationInitializer {

    /** Constructs a simulation initializer with an empty client, given a system initializer.
      */
    def noClient[SystemNetworkMessageT, SystemInputMessageT, ClientMessageT](
        loggerFactory: NamedLoggerFactory,
        timeouts: ProcessingTimeout,
    )(
        systemInitializer: SystemInitializer[
          SimulationEnv,
          SystemNetworkMessageT,
          SystemInputMessageT,
        ]
    ): SimulationInitializer[
      Unit,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ] =
      SimulationInitializer(
        _ => systemInitializer,
        EmptyClient.initializer(loggerFactory, timeouts),
      )
  }

  sealed trait MachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
  ] {
    def initialize(
        onboardingData: OnboardingDataT,
        simulationInitializer: SimulationInitializer[
          OnboardingDataT,
          SystemNetworkMessageT,
          SystemInputMessageT,
          ClientMessageT,
        ],
    ): Machine[OnboardingDataT, SystemNetworkMessageT]
  }

  private def getSimulationName(ref: ModuleRef[?]): ModuleName =
    ref match {
      case SimulationModuleRef(name, _) => name
      case _ =>
        sys.error("Internal error: returned ref that wasn't created by simulation")
    }

  private def addSetBehaviorEvent[MessageT](
      collector: NodeCollector,
      moduleName: ModuleName,
      module: Module[SimulationEnv, MessageT],
      ready: Boolean,
  ): Unit =
    collector.addInternalEvent(moduleName, ModuleControl.SetBehavior(module, ready))

  def apply[OnboardingDataT, SystemNetworkMessageT, SystemInputMessageT, ClientMessageT](
      endpointsToInitializers: Map[
        Endpoint,
        SimulationInitializer[
          OnboardingDataT,
          SystemNetworkMessageT,
          SystemInputMessageT,
          ClientMessageT,
        ],
      ],
      onboardingDataProvider: OnboardingDataProvider[OnboardingDataT],
      config: SimulationSettings,
      clock: SimClock,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Simulation[OnboardingDataT, SystemNetworkMessageT, SystemInputMessageT, ClientMessageT] = {
    val machineInitializer = createMachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ](onboardingDataProvider, timeouts, loggerFactory)
    val (initialSequencersToInitializers, laterOnboardedEndpointsToInitializers) =
      endpointsToInitializers.partition { case (_, initializer) =>
        initializer.initializeImmediately
      }
    val initialSequencersToMachines: Map[SequencerId, Machine[?, ?]] =
      initialSequencersToInitializers.view.map { case (endpoint, simulationInitializer) =>
        val sequencerId = SimulationP2PNetworkManager.fakeSequencerId(endpoint)
        sequencerId -> machineInitializer.initialize(
          onboardingDataProvider.provide(sequencerId),
          simulationInitializer,
        )
      }.toMap
    val topology = Topology(initialSequencersToMachines, laterOnboardedEndpointsToInitializers)
    new Simulation(
      topology,
      onboardingDataProvider,
      machineInitializer,
      config,
      clock,
      loggerFactory,
    )()
  }

  private def createMachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
  ](
      onboardingDataProvider: OnboardingDataProvider[OnboardingDataT],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) =
    new MachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ] {
      override def initialize(
          onboardingData: OnboardingDataT,
          simulationInitializer: SimulationInitializer[
            OnboardingDataT,
            SystemNetworkMessageT,
            SystemInputMessageT,
            ClientMessageT,
          ],
      ): Machine[OnboardingDataT, SystemNetworkMessageT] = {
        val allReactors: mutable.Map[ModuleName, Reactor[?]] = mutable.Map.empty
        val collector = new NodeCollector()
        val system = new SimulationModuleSystem(collector, loggerFactory)

        val simulationP2PNetworkManager =
          SimulationP2PNetworkManager[SystemNetworkMessageT](collector, timeouts, loggerFactory)
        val resultFromInit = simulationInitializer.systemInitializerFactory(onboardingData)(
          system,
          simulationP2PNetworkManager,
        )
        val clientCollector = new ClientCollector(getSimulationName(resultFromInit.inputModuleRef))
        val client = simulationInitializer.clientInitializer.createClient(
          SimulatedRefForClient(clientCollector)
        )
        simulationInitializer.clientInitializer.init(
          SimulationModuleClientContext(clientCollector, loggerFactory)
        )
        val networkInModuleName = getSimulationName(resultFromInit.p2pNetworkInModuleRef)
        val networkOutModuleName = getSimulationName(resultFromInit.p2pNetworkOutAdminModuleRef)

        Machine(
          allReactors,
          networkInModuleName,
          networkOutModuleName,
          collector,
          Reactor(client),
          clientCollector,
          simulationInitializer,
          onboardingDataProvider,
          loggerFactory,
          simulationP2PNetworkManager,
        )
      }
    }
}

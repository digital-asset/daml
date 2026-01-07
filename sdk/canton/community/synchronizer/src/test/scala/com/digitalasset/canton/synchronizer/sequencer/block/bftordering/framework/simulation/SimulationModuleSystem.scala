// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import cats.Traverse
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.endpointToTestBftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  ModuleControl,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager.ReasonForProvide
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  FutureContext,
  Module,
  ModuleContext,
  ModuleName,
  ModuleRef,
  ModuleSystem,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
  PureFun,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.HexString
import org.scalatest.Assertions.fail

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.{Random, Try}

object SimulationModuleSystem {

  private[simulation] final case class SimulationModuleRef[-MessageT](
      name: ModuleName,
      collector: NodeCollector,
  ) extends ModuleRef[MessageT] {

    override def asyncSend(
        msg: MessageT
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      collector.addInternalEvent(name, ModuleControl.Send(msg, traceContext, metricsContext))
  }

  private[simulation] final case class SimulationP2PNetworkRef[P2PMessageT](
      node: BftNodeId,
      collector: NodeCollector,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends P2PNetworkRef[P2PMessageT]
      with NamedLogging {

    override def asyncP2PSend(createMessage: Option[Instant] => P2PMessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit =
      collector.addNetworkEvent(node, createMessage(None))
  }

  final case class SimulationP2PNetworkManager[P2PMessageT](
      collector: NodeCollector,
      p2pConnectionEventListener: P2PConnectionEventListener,
      p2PGrpcConnectionState: P2PGrpcConnectionState,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends P2PNetworkManager[SimulationEnv, P2PMessageT]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        _context: SimulationModuleContext[ActorContextT],
        p2PAddress: P2PAddress,
    )(implicit traceContext: TraceContext): P2PNetworkRef[P2PMessageT] = {
      val maybeP2PEndpoint = p2PAddress.maybeP2PEndpoint
      val node = p2PAddress.maybeBftNodeId.getOrElse(
        maybeP2PEndpoint
          .map(endpointToTestBftNodeId)
          .getOrElse(
            throw new IllegalArgumentException("Either BftNodeId or P2PEndpoint must be provided")
          )
      )
      collector.addOpenConnection(
        node,
        maybeP2PEndpoint match {
          case Some(p2pEndpoint: PlainTextP2PEndpoint) =>
            Some(p2pEndpoint)
          case None => None
          case other =>
            throw new UnsupportedOperationException(
              s"Only plaintext endpoint are supported in simulation, found $other"
            )
        },
        p2pConnectionEventListener,
      )
      SimulationP2PNetworkRef[P2PMessageT](node, collector, timeouts, loggerFactory)
    }

    // We don't currently simulate explicit disconnections
    override def shutdownOutgoingConnection(
        p2pEndpointId: P2PEndpoint.Id
    )(implicit traceContext: TraceContext): Unit = ()
  }

  private[simulation] case object SimulationFutureContext extends FutureContext[SimulationEnv] {
    override def timeFuture[X](timer: Timer, futureUnlessShutdown: => SimulationFuture[X])(implicit
        mc: MetricsContext
    ): SimulationFuture[X] =
      futureUnlessShutdown

    override def pureFuture[X](x: X): SimulationFuture[X] =
      new SimulationFuture.Pure(s"pure($x)", () => Try(x))

    override def mapFuture[X, Y](
        future: SimulationFuture[X]
    )(fun: PureFun[X, Y], orderingStage: Option[String] = None): SimulationFuture[Y] =
      SimulationFuture.Map(future, fun)

    override def zipFuture[X, Y](
        future1: SimulationFuture[X],
        future2: SimulationFuture[Y],
        orderingStage: Option[String] = None,
    ): SimulationFuture[(X, Y)] =
      SimulationFuture.Zip(future1, future2)

    override def zipFuture3[X, Y, Z](
        future1: SimulationFuture[X],
        future2: SimulationFuture[Y],
        future3: SimulationFuture[Z],
        orderingStage: Option[String] = None,
    ): SimulationFuture[(X, Y, Z)] =
      SimulationFuture.Zip3(future1, future2, future3)

    override def sequenceFuture[A, F[_]](
        futures: F[SimulationFuture[A]],
        orderingStage: Option[String] = None,
    )(implicit
        ev: Traverse[F]
    ): SimulationFuture[F[A]] =
      SimulationFuture.Sequence(futures)

    override def flatMapFuture[R1, R2](
        future1: SimulationFuture[R1],
        future2: PureFun[R1, SimulationFuture[R2]],
        orderingStage: Option[String] = None,
    ): SimulationFuture[R2] =
      SimulationFuture.FlatMap(future1, future2)
  }

  private[simulation] trait SimulationModuleContext[MessageT]
      extends ModuleContext[SimulationEnv, MessageT] {

    override def futureContext: FutureContext[SimulationEnv] = SimulationFutureContext

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
    override def cancel()(implicit metricsContext: MetricsContext): Boolean = {
      collector.addCancelTick(tickId)
      true
    }
  }

  final case class TraceContextGenerator(random: Random) {
    private def genHexBytes(n: Int): String = HexString.toHexString(random.nextBytes(n))
    def newTraceContext: TraceContext = {
      val traceParent = s"00-${genHexBytes(16)}-${genHexBytes(8)}-01"
      TraceContext.fromW3CTraceParent(traceParent)
    }

    def ofBatch(items: IterableOnce[HasTraceContext])(logger: TracedLogger): TraceContext = {
      val validTraces =
        items.iterator.map(_.traceContext).filter(_.traceId.isDefined).toSeq.distinct

      NonEmpty.from(validTraces) match {
        case None =>
          newTraceContext // just generate new trace context
        case Some(validTracesNE) =>
          if (validTracesNE.sizeCompare(1) == 0)
            validTracesNE.head1 // there's only a single trace so stick with that
          else {
            implicit val traceContext: TraceContext = newTraceContext
            // log that we're creating a single traceContext from many trace ids
            val traceIds = validTracesNE.map(_.traceId).collect { case Some(traceId) => traceId }
            logger.debug(s"Created batch from traceIds: [${traceIds.mkString(",")}]")
            traceContext
          }
      }
    }
  }

  private[simulation] final case class SimulationModuleNodeContext[MessageT](
      to: ModuleName,
      collector: NodeCollector,
      traceContextGenerator: TraceContextGenerator,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override val self: SimulationModuleRef[MessageT] = SimulationModuleRef(to, collector)

    override def delayedEvent(delay: FiniteDuration, message: MessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): CancellableEvent = {
      val tickCounter =
        collector.addTickEvent(delay, to, ModuleControl.Send(message, traceContext, metricsContext))
      SimulationCancelable(collector, tickCounter)
    }

    override def pipeToSelfInternal[X](
        futureUnlessShutdown: SimulationFuture[X]
    )(
        fun: Try[X] => Option[MessageT]
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      collector.addFuture(to, futureUnlessShutdown, fun)

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    )(moduleNameForMetrics: String = moduleName.name): SimulationModuleRef[NewModuleMessageT] =
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

    override def withNewTraceContext[A](fn: TraceContext => A): A =
      fn(traceContextGenerator.newTraceContext)

    override def traceContextOfBatch(items: IterableOnce[HasTraceContext]): TraceContext =
      traceContextGenerator.ofBatch(items)(logger)
  }

  private[simulation] final case class SimulationModuleClientContext[MessageT](
      collector: ClientCollector,
      traceContextGenerator: TraceContextGenerator,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    )(moduleNameForMetrics: String = moduleName.name): SimulationModuleRef[NewModuleMessageT] =
      unsupportedForClientModules()

    override def setModule[NewModuleMessageT](
        moduleRef: SimulationModuleRef[NewModuleMessageT],
        module: Module[SimulationEnv, NewModuleMessageT],
    ): Unit =
      unsupportedForClientModules()

    override def self: SimulationModuleRef[MessageT] = unsupportedForClientModules()

    override def delayedEvent(delay: FiniteDuration, message: MessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): CancellableEvent = {
      val tickId = collector.addTickEvent(delay, message)
      SimulationCancelable(collector, tickId)
    }

    override def pipeToSelfInternal[X](futureUnlessShutdown: SimulationFuture[X])(
        fun: Try[X] => Option[MessageT]
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      unsupportedForClientModules()

    override def become(module: Module[SimulationEnv, MessageT]): Unit =
      unsupportedForClientModules()

    override def stop(onStop: () => Unit): Unit =
      unsupportedForClientModules()

    override def withNewTraceContext[A](fn: TraceContext => A): A = fn(
      traceContextGenerator.newTraceContext
    )

    override def traceContextOfBatch(items: IterableOnce[HasTraceContext]): TraceContext =
      traceContextGenerator.ofBatch(items)(logger)

    private def unsupportedForClientModules(): Nothing =
      sys.error("Unsupported for client modules")

  }

  private[simulation] final class SimulationModuleSystemContext[MessageT](
      collector: NodeCollector,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimulationModuleContext[MessageT] {

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    )(moduleNameForMetrics: String = moduleName.name): SimulationModuleRef[NewModuleMessageT] =
      SimulationModuleRef(moduleName, collector)

    override def setModule[NewModuleMessageT](
        moduleRef: SimulationModuleRef[NewModuleMessageT],
        module: Module[SimulationEnv, NewModuleMessageT],
    ): Unit =
      addSetBehaviorEvent(collector, moduleRef.name, module, ready = false)

    override def self: SimulationModuleRef[MessageT] = unsupportedForSystem()

    override def delayedEvent(delay: FiniteDuration, message: MessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): CancellableEvent =
      unsupportedForSystem()

    override def pipeToSelfInternal[X](futureUnlessShutdown: SimulationFuture[X])(
        fun: Try[X] => Option[MessageT]
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      unsupportedForSystem()

    private def unsupportedForSystem(): Nothing =
      sys.error("Unsupported for system object")

    override def become(module: Module[SimulationEnv, MessageT]): Unit = unsupportedForSystem()

    override def stop(onStop: () => Unit): Unit = unsupportedForSystem()

    override def withNewTraceContext[A](fn: TraceContext => A): A = unsupportedForSystem()

    override def traceContextOfBatch(items: IterableOnce[HasTraceContext]): TraceContext =
      unsupportedForSystem()
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

    override def futureContext: FutureContext[SimulationEnv] = SimulationFutureContext

    override def newModuleRef[MessageT](
        moduleName: ModuleName // Must be unique per ref, else it will crash on spawn
    )(moduleNameForMetrics: String = moduleName.name): SimulationModuleRef[MessageT] =
      SimulationModuleRef(moduleName, collector)

    override def setModule[MessageT](
        moduleRef: SimulationModuleRef[MessageT],
        module: Module[SimulationEnv, MessageT],
    ): Unit =
      addSetBehaviorEvent(collector, moduleRef.name, module, ready = false)
  }

  private final case class SimulatedRefForClient[MessageT](collector: ClientCollector)
      extends ModuleRef[MessageT] {
    override def asyncSend(
        msg: MessageT
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      collector.addClientRequest(msg)
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
        SimulationP2PNetworkManager[SystemNetworkMessageT],
        SystemNetworkMessageT,
        SystemInputMessageT,
      ],
      clientInitializer: SimulationClient.Initializer[
        SimulationEnv,
        ClientMessageT,
        SystemInputMessageT,
      ],
      p2pGrpcConnectionState: P2PGrpcConnectionState,
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
          SimulationP2PNetworkManager[SystemNetworkMessageT],
          SystemNetworkMessageT,
          SystemInputMessageT,
        ],
        p2pGrpcConnectionState: P2PGrpcConnectionState,
    ): SimulationInitializer[
      Unit,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ] =
      SimulationInitializer(
        _ => systemInitializer,
        EmptyClient.initializer(loggerFactory, timeouts),
        p2pGrpcConnectionState,
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
        P2PEndpoint,
        SimulationInitializer[
          OnboardingDataT,
          SystemNetworkMessageT,
          SystemInputMessageT,
          ClientMessageT,
        ],
      ],
      onboardingManager: OnboardingManager[OnboardingDataT],
      config: SimulationSettings,
      clock: SimClock,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Simulation[OnboardingDataT, SystemNetworkMessageT, SystemInputMessageT, ClientMessageT] = {
    val traceContextGenerator = TraceContextGenerator(new Random(config.localSettings.randomSeed))
    val machineInitializer = createMachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ](onboardingManager, timeouts, loggerFactory, traceContextGenerator)
    val (initialSequencersToInitializers, laterOnboardedEndpointsToInitializers) =
      endpointsToInitializers.partition { case (_, initializer) =>
        initializer.initializeImmediately
      }
    val initialSequencersToMachines: Map[BftNodeId, Machine[?, ?]] =
      initialSequencersToInitializers.view.map { case (endpoint, simulationInitializer) =>
        val node = endpointToTestBftNodeId(endpoint)
        node -> machineInitializer.initialize(
          onboardingManager.provide(ReasonForProvide.ProvideForInit, node),
          simulationInitializer,
        )
      }.toMap
    val topology = Topology(initialSequencersToMachines, laterOnboardedEndpointsToInitializers)
    new Simulation(
      topology,
      onboardingManager,
      machineInitializer,
      config,
      clock,
      traceContextGenerator,
      loggerFactory,
    )()
  }

  private def createMachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
  ](
      onboardingManager: OnboardingManager[OnboardingDataT],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      traceContextGenerator: TraceContextGenerator,
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

        val resultFromInit = simulationInitializer
          .systemInitializerFactory(onboardingData)
          .initialize(
            system,
            (p2pConnectionEventListener, _) =>
              SimulationP2PNetworkManager[SystemNetworkMessageT](
                collector,
                p2pConnectionEventListener,
                simulationInitializer.p2pGrpcConnectionState,
                timeouts,
                loggerFactory,
              ),
          )
        val clientCollector = new ClientCollector(getSimulationName(resultFromInit.inputModuleRef))
        val client = simulationInitializer.clientInitializer.createClient(
          SimulatedRefForClient(clientCollector)
        )
        simulationInitializer.clientInitializer.init(
          SimulationModuleClientContext(clientCollector, traceContextGenerator, loggerFactory)
        )
        val mempoolModuleName = getSimulationName(resultFromInit.inputModuleRef)
        val outputModuleName = getSimulationName(resultFromInit.outputModuleRef)
        val networkInModuleName = getSimulationName(resultFromInit.p2pNetworkInModuleRef)
        val networkOutModuleName = getSimulationName(resultFromInit.p2pNetworkOutAdminModuleRef)

        Machine(
          allReactors,
          mempoolModuleName,
          outputModuleName,
          networkInModuleName,
          networkOutModuleName,
          collector,
          Reactor(client),
          clientCollector,
          simulationInitializer,
          onboardingManager,
          loggerFactory,
          resultFromInit.p2pNetworkManager,
        )
      }
    }
}

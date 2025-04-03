// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl.Send
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.RetransmissionsMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.P2PNetworkOut
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.Simulation.endpointToNode
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  MachineInitializer,
  SimulationEnv,
  SimulationInitializer,
  SimulationModuleSystem,
  SimulationP2PNetworkManager,
  TraceContextGenerator,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.RunningFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager.ReasonForProvide.{
  ProvideForInit,
  ProvideForRestart,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Module,
  ModuleName,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import pprint.{PPrinter, Tree}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** @param clock
  *   Due to using the [[com.digitalasset.canton.time.SimClock]] and
  *   [[com.digitalasset.canton.data.CantonTimestamp]]s, the time resolution needs to be at least
  *   microseconds. Otherwise, commands might be scheduled with timestamps that are the same from
  *   the [[com.digitalasset.canton.time.SimClock]] point of view. It may result in executing
  *   commands in an incorrect order.
  */
class Simulation[OnboardingDataT, SystemNetworkMessageT, SystemInputMessageT, ClientMessageT](
    private var topology: Topology[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ],
    onboardingManager: OnboardingManager[OnboardingDataT],
    machineInitializer: MachineInitializer[
      OnboardingDataT,
      SystemNetworkMessageT,
      SystemInputMessageT,
      ClientMessageT,
    ],
    simSettings: SimulationSettings,
    clock: SimClock,
    traceContextGenerator: TraceContextGenerator,
    loggerFactory: NamedLoggerFactory,
)(val agenda: Agenda = new Agenda(clock, loggerFactory)) {

  val simulationStageStart: CantonTimestamp = clock.now

  // onboarding
  onboardingManager.initCommands.foreach { case (command, at) =>
    agenda.addOne(command, at, ScheduledCommand.DefaultPriority)
  }

  agenda.addOne(MakeSystemHealthy, simSettings.durationOfFirstPhaseWithFaults)
  // Schedule liveness checks starting from "phase 2" up to the end of the simulation
  LazyList
    .iterate(simSettings.durationOfFirstPhaseWithFaults + simSettings.livenessCheckInterval)(
      _ + simSettings.livenessCheckInterval
    )
    .takeWhile(_ < simSettings.totalSimulationTime)
    .foreach(at => agenda.addOne(ResumeLivenessChecks, at))
  agenda.addOne(Quit("End of time"), simSettings.totalSimulationTime)

  private val network = new NetworkSimulator(simSettings.networkSettings, topology, agenda, clock)
  private val local =
    new LocalSimulator(
      simSettings.localSettings,
      // TODO(#22807): Currently, only initial nodes are subjects to crashes.
      nodes = topology.activeSequencersToMachines.view.keySet.toSet,
      agenda,
    )

  // the init functions might have already sent messages that we need to add to the agenda
  topology.foreach { (node, machine) =>
    runNodeCollector(node, EventOriginator.FromInit, machine.nodeCollector)
    runClientCollector(node, machine.clientCollector)
  }

  private type History = Seq[Command]
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val currentHistory: mutable.ArrayBuffer[Command] = mutable.ArrayBuffer.empty[Command]

  private val logger = loggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def nextThingTodo(): ScheduledCommand = {
    if (agenda.isEmpty) {
      return ScheduledCommand(Quit("Nothing left to simulate"), clock.now, -1)
    }

    agenda.dequeue()
  }

  private def runNodeCollector(
      node: BftNodeId,
      from: EventOriginator,
      collector: NodeCollector,
  ): Unit =
    collector.collect().foreach {
      case NodeCollector.InternalEvent(sendTo, msg) =>
        local.scheduleEvent(node, sendTo, from, msg)
      case NodeCollector.TickEvent(duration, tickId, to, msg) =>
        local.scheduleTick(node, to, tickId, duration, msg)
      case NodeCollector.SendNetworkEvent(to, msg, traceContext) =>
        network.scheduleNetworkEvent(from = node, to, msg, traceContext)
      case NodeCollector.AddFuture(to, future, errorMessage, traceContext) =>
        local.scheduleFuture(node, to, clock.now, future, errorMessage, traceContext)
      case NodeCollector.CancelTick(tickCounter) =>
        agenda.removeInternalTick(node, tickCounter)
      case NodeCollector.OpenConnection(to, endpoint, continuation) =>
        network.scheduleEstablishConnection(node, to, endpoint, continuation)
    }

  private def runClientCollector(node: BftNodeId, collector: ClientCollector): Unit =
    collector.collect().foreach {
      case ClientCollector.TickEvent(duration, tickId, newMsg, traceContext) =>
        local.scheduleClientTick(node, tickId, duration, newMsg, traceContext)
      case ClientCollector.ClientRequest(to, msg, traceContext) =>
        local.scheduleEvent(
          node,
          to,
          EventOriginator.FromClient,
          ModuleControl.Send(msg, traceContext),
        )
      case ClientCollector.CancelTick(tickCounter) =>
        agenda.removeClientTick(node, tickCounter)
    }

  private def executeEvent[MessageT](
      node: BftNodeId,
      toAddress: ModuleAddress,
      msg: ModuleControl[SimulationEnv, MessageT],
  ): Unit = {
    val machine = tryGetMachine(node)
    val to = machine.resolveModuleAddress(toAddress)
    val context =
      SimulationModuleSystem.SimulationModuleNodeContext[MessageT](
        to,
        machine.nodeCollector,
        traceContextGenerator,
        loggerFactory,
      )
    msg match {

      case ModuleControl.Send(message, traceContext) =>
        tryGetReactor(node, machine, to, msg).foreach { reactor =>
          val module = asModule[MessageT](reactor)
          module.receive(message)(context, traceContext)
        }

      case ModuleControl.SetBehavior(module, ready) =>
        machine.allReactors.addOne(to -> Reactor(module))
        if (ready)
          module.ready(context.self)
        logger.info(s"$node has set a behavior for module $to (ready=$ready)")

      case ModuleControl.Stop(onStop) =>
        onStop()
        val _ = machine.allReactors.remove(to)
        logger.info(s"$node has stopped module $to")

      case ModuleControl.NoOp() =>
    }

    runNodeCollector(node, EventOriginator.FromInternalModule(to), machine.nodeCollector)
  }

  private def executeFuture[FutureT, MessageT](
      node: BftNodeId,
      name: ModuleName,
      future: RunningFuture[FutureT],
      fun: Try[FutureT] => Option[MessageT],
      traceContext: TraceContext,
  ): Unit =
    future.resolveAllBelow(clock.now) match {
      case RunningFuture.Scheduled(nextTime, newFuture) =>
        agenda.addOne(
          RunFuture(node, name, newFuture, fun, traceContext),
          nextTime,
          ScheduledCommand.DefaultPriority,
        )
      case RunningFuture.Resolved(valueFromFuture) =>
        fun(valueFromFuture).foreach { msg =>
          local.scheduleEvent(
            node,
            name,
            EventOriginator.FromFuture,
            ModuleControl.Send(msg, traceContext),
          )
        }
    }

  private def executeClientTick[M](node: BftNodeId, msg: M, traceContext: TraceContext): Unit = {
    val machine = tryGetMachine(node)
    asModule[M](machine.clientReactor)
      .receive(msg)(
        SimulationModuleSystem
          .SimulationModuleClientContext(
            machine.clientCollector,
            traceContextGenerator,
            loggerFactory,
          ),
        traceContext,
      )
    runClientCollector(node, machine.clientCollector)
  }

  private def startMachine(
      endpoint: P2PEndpoint
  ): BftNodeId = {
    val node = endpointToNode(endpoint)
    val initializer = topology.laterOnboardedEndpointsWithInitializers(endpoint)
    val onboardingData = onboardingManager.provide(ProvideForInit, node)
    val machine = machineInitializer.initialize(onboardingData, initializer)
    topology = topology.copy(activeSequencersToMachines =
      topology.activeSequencersToMachines.updated(node, machine)
    )
    // handle init messages
    runNodeCollector(node, EventOriginator.FromInit, machine.nodeCollector)
    node
  }

  private def crashRestartNode(node: BftNodeId): Unit = {
    agenda.removeCommandsOnCrash(node)
    val machine = tryGetMachine(node)
    machine.crashRestart(node)
    runNodeCollector(node, EventOriginator.FromInit, machine.nodeCollector)
  }

  private def addEndpoint(endpoint: P2PEndpoint, to: BftNodeId): Unit = {
    logger.debug(s"immediately executing addEndpoint for $to -> $endpoint")
    executeEvent(
      to,
      ModuleAddress.NetworkOut,
      ModuleControl.Send(
        P2PNetworkOut.Admin.AddEndpoint(
          endpoint,
          added =>
            if (!added)
              throw new IllegalStateException(s"Endpoint $endpoint has not been added to $to"),
        ),
        TraceContext.empty,
      ),
    )
  }

  // Fills in the message type; for documentation purposes only, due to generics erasure.
  private def asModule[M](reactor: Reactor[?]): Module[SimulationEnv, M] =
    reactor.module.asInstanceOf[Module[SimulationEnv, M]]

  private def tryGetMachine(node: BftNodeId): Machine[?, ?] =
    topology.getMachine(node).getOrElse(throw new IllegalStateException(s"Unknown node $node"))

  private def tryGetReactor[MessageT](
      node: BftNodeId,
      machine: Machine[?, ?],
      to: ModuleName,
      msg: ModuleControl[SimulationEnv, MessageT],
  ): Option[Reactor[?]] =
    machine.allReactors.get(to) match {
      // TODO(#23433) revisit
      case None =>
        msg match {
          // TODO(#23434) check if can be fixed differently
          case Send(RetransmissionsMessage.StatusRequest(_), _) =>
            // We don't care about status requests after the module is gone
            None
          case _ =>
            throw new IllegalStateException(
              s"On node '$node': unknown target module $to for event $msg"
            )
        }
      case someReactor => someReactor
    }

  def addCommands(commands: Seq[(Command, FiniteDuration)]): Unit = commands.foreach {
    case (command, duration) =>
      agenda.addOne(command, duration)
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def run(verifier: SimulationVerifier = NoVerification): History = {
    logger.debug(
      s"Starting simulation with these settings:\n${PPrinter(additionalHandlers = fixupDurationPrettyPrinting)(simSettings)}}"
    )

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var continueToRun = true
    try {
      while (continueToRun) {
        val whatToDo = nextThingTodo()

        clock.advanceTo(whatToDo.at, logAdvancement = false)(TraceContext.empty)
        local.tick(clock.now)
        network.tick()
        val _ = currentHistory.addOne(whatToDo.command)

        whatToDo.command match {
          case InternalEvent(machineName, to, _, msg) =>
            executeEvent(machineName, ModuleAddress.ViaName(to), msg)
          case InjectedSend(machineName, to, _, msg) =>
            executeEvent(machineName, to, ModuleControl.Send(msg, TraceContext.empty))
          case InternalTick(machineName, to, _, msg) =>
            executeEvent(machineName, ModuleAddress.ViaName(to), msg)
          case RunFuture(machine, to, toRun, fun, traceContext) =>
            logger.trace(s"Future ${toRun.name} for $machine:$to completed")
            executeFuture(machine, to, toRun, fun, traceContext)
            verifier.aFutureHappened(machine)
          case ReceiveNetworkMessage(machineName, msg, traceContext) =>
            local.scheduleEvent(
              machineName,
              tryGetMachine(machineName).networkInReactor,
              EventOriginator.FromNetwork,
              ModuleControl.Send(msg, traceContext),
            )
          case ClientTick(machine, _, msg, traceContext) =>
            logger.info(s"Client for $machine ticks")
            executeClientTick(machine, msg, traceContext)
          case StartMachine(endpoint) =>
            val node = startMachine(endpoint)
            verifier.nodeStarted(clock.now, node)
            addCommands(onboardingManager.machineStarted(clock.now, endpoint, node))
          case PrepareOnboarding(node) =>
            addCommands(onboardingManager.prepareOnboardingFor(clock.now, node))
          case AddEndpoint(endpoint, to) =>
            addEndpoint(endpoint, to)
          case EstablishConnection(from, to, endpoint, continuation) =>
            logger.debug(s"Establish connection '$from' -> '$to' via $endpoint")
            continuation(endpoint.id, to)
            val machine = tryGetMachine(from)
            runNodeCollector(from, EventOriginator.FromNetwork, machine.nodeCollector)
          case CrashRestartNode(node) =>
            logger.info(s"Crashing '$node'")
            crashRestartNode(node)
          case MakeSystemHealthy =>
            local.makeHealthy()
            network.makeHealthy()
            verifier.resumeCheckingLiveness(clock.now)
          case ResumeLivenessChecks =>
            verifier.resumeCheckingLiveness(clock.now)
          case Quit(reason) =>
            logger.debug(s"Stopping simulation because: $reason")
            continueToRun = false
        }

        verifier.checkInvariants(clock.now)
        addCommands(onboardingManager.commandsToSchedule(clock.now))
      }
    } catch {
      case e: Throwable =>
        logger.error(
          s"Uncaught exception during simulation, it failed with these settings:\n${PPrinter(additionalHandlers = fixupDurationPrettyPrinting)(simSettings)}",
          e,
        )
        throw e
    }
    currentHistory.toSeq
  }

  def newStage(
      simulationSettings: SimulationSettings,
      onboardingDataProvider: OnboardingManager[OnboardingDataT],
      newlyOnboardedEndpointsToInitializers: Map[
        P2PEndpoint,
        SimulationInitializer[
          OnboardingDataT,
          SystemNetworkMessageT,
          SystemInputMessageT,
          ClientMessageT,
        ],
      ],
  ): Simulation[OnboardingDataT, SystemNetworkMessageT, SystemInputMessageT, ClientMessageT] = {
    val newSim =
      new Simulation(
        topology.copy(
          laterOnboardedEndpointsWithInitializers = newlyOnboardedEndpointsToInitializers
        ),
        onboardingDataProvider,
        machineInitializer,
        simulationSettings,
        clock,
        traceContextGenerator,
        loggerFactory,
      )(agenda)
    newSim
  }

  private def fixupDurationPrettyPrinting: PartialFunction[Any, Tree] = {
    case duration: java.time.Duration =>
      Tree.Literal(s"Duration.ofNanos(${duration.toNanos}L)")
  }
}

object Simulation {

  def endpointToNode(endpoint: P2PEndpoint): BftNodeId =
    BftNodeId(endpoint.id.url)
}

final case class Reactor[InnerMessage](module: Module[SimulationEnv, InnerMessage])

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class Machine[OnboardingDataT, SystemNetworkMessageT](
    allReactors: mutable.Map[ModuleName, Reactor[?]],
    mempoolReactor: ModuleName,
    outputReactor: ModuleName,
    networkInReactor: ModuleName,
    networkOutReactor: ModuleName,
    nodeCollector: NodeCollector,
    clientReactor: Reactor[?],
    clientCollector: ClientCollector,
    init: SimulationInitializer[OnboardingDataT, SystemNetworkMessageT, ?, ?],
    onboardingManager: OnboardingManager[OnboardingDataT],
    loggerFactory: NamedLoggerFactory,
    simulationP2PNetworkManager: SimulationP2PNetworkManager[SystemNetworkMessageT],
) {
  private val logger = loggerFactory.getLogger(getClass)

  def crashRestart(node: BftNodeId): Unit = {
    logger.info("Stopping modules to simulate crash")
    allReactors.clear()
    val system = new SimulationModuleSystem(nodeCollector, loggerFactory)
    logger.info("Initializing modules again to simulate restart")
    val _ = init
      .systemInitializerFactory(onboardingManager.provide(ProvideForRestart, node))
      .initialize(system, simulationP2PNetworkManager)
  }

  def resolveModuleAddress(toAddress: ModuleAddress) = toAddress match {
    case ModuleAddress.ViaName(moduleName) => moduleName
    case ModuleAddress.Mempool => mempoolReactor
    case ModuleAddress.Output => outputReactor
    case ModuleAddress.NetworkIn => networkInReactor
    case ModuleAddress.NetworkOut => networkOutReactor
  }
}

final case class Topology[
    OnboardingDataT,
    SystemNetworkMessageT,
    SystemInputMessageT,
    ClientMessageT,
](
    activeSequencersToMachines: Map[BftNodeId, Machine[?, ?]],
    laterOnboardedEndpointsWithInitializers: Map[
      P2PEndpoint,
      SimulationInitializer[
        OnboardingDataT,
        SystemNetworkMessageT,
        SystemInputMessageT,
        ClientMessageT,
      ],
    ],
) {
  lazy val activeNonInitialEndpoints: Seq[P2PEndpoint] =
    laterOnboardedEndpointsWithInitializers
      .filter { case (endpoint, _) =>
        val nodeId = endpointToNode(endpoint)
        activeSequencersToMachines.contains(nodeId)
      }
      .keys
      .toSeq

  def getMachine(node: BftNodeId): Option[Machine[?, ?]] = activeSequencersToMachines.get(node)
  def foreach(f: (BftNodeId, Machine[?, ?]) => Unit): Unit =
    activeSequencersToMachines.foreach(f.tupled)
}

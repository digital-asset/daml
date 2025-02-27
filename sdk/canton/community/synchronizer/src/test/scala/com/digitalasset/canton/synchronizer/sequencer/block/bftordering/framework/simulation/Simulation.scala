// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl.Send
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.RetransmissionsMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.P2PNetworkOut
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  MachineInitializer,
  SimulationEnv,
  SimulationInitializer,
  SimulationModuleSystem,
  SimulationP2PNetworkManager,
  TraceContextGenerator,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.RunningFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingDataProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Module,
  ModuleName,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.{
  onboardingTime,
  sequencerBecomeOnlineTime,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.{Logger, LoggerFactory}
import pprint.{PPrinter, Tree}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
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
    onboardingDataProvider: OnboardingDataProvider[OnboardingDataT],
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
)(val agenda: Agenda = new Agenda(clock)) {

  val simulationStageStart: CantonTimestamp = clock.now

  // onboarding
  simSettings.peerOnboardingDelays
    .zip(topology.laterOnboardedEndpointsWithInitializers)
    .foldLeft(Map[TopologyActivationTime, Seq[PlainTextP2PEndpoint]]()) {
      case (acc, (onboardingDelay, (endpoint, _))) =>
        val activationTime = onboardingTime(simulationStageStart, onboardingDelay)
        acc.get(activationTime) match {
          case Some(endpoints) => acc + (activationTime -> (endpoints :+ endpoint))
          case None => acc + (activationTime -> Seq(endpoint))
        }
    }
    .foreach { case (onboardingTime, endpoints) =>
      agenda.addOne(
        OnboardSequencers(endpoints),
        at = sequencerBecomeOnlineTime(onboardingTime, simSettings),
        ScheduledCommand.DefaultPriority,
      )
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
      // TODO(#22807): Currently, only initial peers are subjects to crashes.
      peers = topology.activeSequencersToMachines.view.keySet.toSet,
      agenda,
    )

  // the init functions might have already sent messages that we need to add to the agenda
  topology.foreach { (peer, machine) =>
    runNodeCollector(peer, FromInit, machine.nodeCollector)
    runClientCollector(peer, machine.clientCollector)
  }

  private type History = Seq[Command]
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val currentHistory: mutable.ArrayBuffer[Command] = mutable.ArrayBuffer.empty[Command]

  implicit private val logger: Logger = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def nextThingTodo(): ScheduledCommand = {
    if (agenda.isEmpty) {
      return ScheduledCommand(Quit("Nothing left to simulate"), clock.now, -1)
    }

    agenda.dequeue()
  }

  private def runNodeCollector(
      peer: SequencerId,
      from: EventOriginator,
      collector: NodeCollector,
  ): Unit =
    collector.collect().foreach {
      case NodeCollector.InternalEvent(sendTo, msg) =>
        local.scheduleEvent(peer, sendTo, from, msg)
      case NodeCollector.TickEvent(duration, tickId, to, msg) =>
        local.scheduleTick(peer, to, tickId, duration, msg)
      case NodeCollector.SendNetworkEvent(toPeer, msg, traceContext) =>
        network.scheduleNetworkEvent(fromPeer = peer, toPeer, msg, traceContext)
      case NodeCollector.AddFuture(to, future, errorMessage, traceContext) =>
        local.scheduleFuture(peer, to, clock.now, future, errorMessage, traceContext)
      case NodeCollector.CancelTick(tickCounter) =>
        agenda.removeInternalTick(peer, tickCounter)
      case NodeCollector.OpenConnection(sequencerId, endpoint, continuation) =>
        network.scheduleEstablishConnection(peer, sequencerId, endpoint, continuation)
    }

  private def runClientCollector(peer: SequencerId, collector: ClientCollector): Unit =
    collector.collect().foreach {
      case ClientCollector.TickEvent(duration, tickId, newMsg) =>
        local.scheduleClientTick(peer, tickId, duration, newMsg)
      case ClientCollector.ClientRequest(to, msg, traceContext) =>
        local.scheduleEvent(peer, to, FromClient, ModuleControl.Send(msg, traceContext))
      case ClientCollector.CancelTick(tickCounter) =>
        agenda.removeClientTick(peer, tickCounter)
    }

  private def executeEvent[MessageT](
      peer: SequencerId,
      to: ModuleName,
      msg: ModuleControl[SimulationEnv, MessageT],
  ): Unit = {
    val machine = tryGetMachine(peer)
    val context =
      SimulationModuleSystem.SimulationModuleNodeContext[MessageT](
        to,
        machine.nodeCollector,
        traceContextGenerator,
        loggerFactory,
      )
    msg match {

      case ModuleControl.Send(message, traceContext) =>
        tryGetReactor(peer, machine, to, msg).foreach { reactor =>
          val module = asModule[MessageT](reactor)
          module.receive(message)(context, traceContext)
        }

      case ModuleControl.SetBehavior(module, ready) =>
        machine.allReactors.addOne(to -> Reactor(module))
        if (ready)
          module.ready(context.self)
        logger.info(s"$peer has set a behavior for module $to (ready=$ready)")

      case ModuleControl.Stop(onStop) =>
        onStop()
        val _ = machine.allReactors.remove(to)
        logger.info(s"$peer has stopped module $to")

      case ModuleControl.NoOp() =>
    }

    runNodeCollector(peer, FromInternalModule(to), machine.nodeCollector)
  }

  private def executeFuture[FutureT, MessageT](
      peer: SequencerId,
      name: ModuleName,
      future: RunningFuture[FutureT],
      fun: Try[FutureT] => Option[MessageT],
      traceContext: TraceContext,
  ): Unit =
    future.resolveAllBelow(clock.now) match {
      case RunningFuture.Scheduled(nextTime, newFuture) =>
        agenda.addOne(
          RunFuture(peer, name, newFuture, fun, traceContext),
          nextTime,
          ScheduledCommand.DefaultPriority,
        )
      case RunningFuture.Resolved(valueFromFuture) =>
        fun(valueFromFuture).foreach { msg =>
          local.scheduleEvent(peer, name, FromFuture, ModuleControl.Send(msg, traceContext))
        }
    }

  private def executeClientTick[M](peer: SequencerId, msg: M): Unit = {
    val machine = tryGetMachine(peer)
    asModule[M](machine.clientReactor)
      .receive(msg)(
        SimulationModuleSystem
          .SimulationModuleClientContext(
            machine.clientCollector,
            traceContextGenerator,
            loggerFactory,
          ),
        TraceContext.empty,
      )
    runClientCollector(peer, machine.clientCollector)
  }

  private def crashRestartPeer(peer: SequencerId): Unit = {
    agenda.removeCommandsOnCrash(peer)
    val machine = tryGetMachine(peer)
    machine.crashRestart(peer)
    runNodeCollector(peer, FromInit, machine.nodeCollector)
  }

  private def onboardSequencers(endpoints: Seq[PlainTextP2PEndpoint]): Unit = {
    val endpointToSequencerId = endpoints.view
      .map(endpoint => endpoint -> SimulationP2PNetworkManager.fakeSequencerId(endpoint))
      .toMap

    logger.info(s"Onboarding new sequencers ${endpointToSequencerId.values}")

    // add endpoints to existing peers
    endpoints.foreach { endpoint =>
      topology.foreach { case (peerId, _) =>
        addEndpoint(endpoint, peerId)
      }
    }

    // initialize
    endpoints.foreach { endpoint =>
      val sequencerId = endpointToSequencerId(endpoint)
      val initializer = topology.laterOnboardedEndpointsWithInitializers(endpoint)
      val onboardingData = onboardingDataProvider.provide(sequencerId)
      val machine = machineInitializer.initialize(onboardingData, initializer)
      topology = topology.copy(activeSequencersToMachines =
        topology.activeSequencersToMachines.updated(sequencerId, machine)
      )
      // handle init messages
      runNodeCollector(sequencerId, FromInit, machine.nodeCollector)
    }

    // add endpoints to new peers
    endpoints.foreach { endpoint1 =>
      endpoints.foreach { endpoint2 =>
        if (endpoint1 != endpoint2) {
          val sequencerId = endpointToSequencerId(endpoint1)
          // needs to happen after handling init messages (setting behaviors for modules)
          agenda.addOne(
            AddEndpoint(endpoint2, sequencerId),
            duration = 1.microsecond,
            ScheduledCommand.DefaultPriority,
          )
        }
      }
    }
  }

  private def addEndpoint(endpoint: P2PEndpoint, to: SequencerId): Unit =
    executeEvent(
      to,
      tryGetMachine(to).networkOutReactor,
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

  // Fills in the message type; for documentation purposes only, due to generics erasure.
  private def asModule[M](reactor: Reactor[?]): Module[SimulationEnv, M] =
    reactor.module.asInstanceOf[Module[SimulationEnv, M]]

  private def tryGetMachine(peer: SequencerId): Machine[?, ?] =
    topology.getMachine(peer).getOrElse(throw new IllegalStateException(s"Unknown peer $peer"))

  private def tryGetReactor[MessageT](
      peer: SequencerId,
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
              s"On peer $peer: unknown target module $to for event $msg"
            )
        }
      case someReactor => someReactor
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
            executeEvent(machineName, to, msg)
          case InternalTick(machineName, to, _, msg) =>
            executeEvent(machineName, to, msg)
          case RunFuture(machine, to, toRun, fun, traceContext) =>
            logger.info(s"Future ${toRun.name} for $machine:$to completed")
            executeFuture(machine, to, toRun, fun, traceContext)
            verifier.aFutureHappened(machine)
          case ReceiveNetworkMessage(machineName, msg, traceContext) =>
            local.scheduleEvent(
              machineName,
              tryGetMachine(machineName).networkInReactor,
              FromNetwork,
              ModuleControl.Send(msg, traceContext),
            )
          case ClientTick(machine, _, msg) =>
            logger.info(s"Client for $machine ticks")
            executeClientTick(machine, msg)
          case OnboardSequencers(endpoints) =>
            onboardSequencers(endpoints)
          case AddEndpoint(endpoint, to) =>
            addEndpoint(endpoint, to)
          case EstablishConnection(fromPeer, toPeer, endpoint, continuation) =>
            logger.debug(s"Establish connection $fromPeer -> $toPeer via $endpoint")
            continuation(endpoint.id, toPeer)
            val machine = tryGetMachine(fromPeer)
            runNodeCollector(fromPeer, FromNetwork, machine.nodeCollector)
          case CrashRestartPeer(peer) =>
            logger.info(s"Crashing $peer")
            crashRestartPeer(peer)
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
      onboardingDataProvider: OnboardingDataProvider[OnboardingDataT],
      newlyOnboardedEndpointsToInitializers: Map[
        PlainTextP2PEndpoint,
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

final case class Reactor[InnerMessage](module: Module[SimulationEnv, InnerMessage])

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class Machine[OnboardingDataT, SystemNetworkMessageT](
    allReactors: mutable.Map[ModuleName, Reactor[?]],
    networkInReactor: ModuleName,
    networkOutReactor: ModuleName,
    nodeCollector: NodeCollector,
    clientReactor: Reactor[?],
    clientCollector: ClientCollector,
    init: SimulationInitializer[OnboardingDataT, SystemNetworkMessageT, ?, ?],
    onboardingDataProvider: OnboardingDataProvider[OnboardingDataT],
    loggerFactory: NamedLoggerFactory,
    simulationP2PNetworkManager: SimulationP2PNetworkManager[SystemNetworkMessageT],
) {
  implicit private val logger: Logger = LoggerFactory.getLogger(getClass)

  def crashRestart(peer: SequencerId): Unit = {
    logger.info("Stopping modules to simulate crash")
    allReactors.clear()
    val system = new SimulationModuleSystem(nodeCollector, loggerFactory)
    logger.info("Initializing modules again to simulate restart")
    val _ = init
      .systemInitializerFactory(onboardingDataProvider.provide(peer))
      .initialize(system, simulationP2PNetworkManager)
  }
}

final case class Topology[
    OnboardingDataT,
    SystemNetworkMessageT,
    SystemInputMessageT,
    ClientMessageT,
](
    activeSequencersToMachines: Map[SequencerId, Machine[?, ?]],
    laterOnboardedEndpointsWithInitializers: Map[
      PlainTextP2PEndpoint,
      SimulationInitializer[
        OnboardingDataT,
        SystemNetworkMessageT,
        SystemInputMessageT,
        ClientMessageT,
      ],
    ],
) {
  def getMachine(peer: SequencerId): Option[Machine[?, ?]] = activeSequencersToMachines.get(peer)
  def foreach(f: (SequencerId, Machine[?, ?]) => Unit): Unit =
    activeSequencersToMachines.foreach(f.tupled)
}

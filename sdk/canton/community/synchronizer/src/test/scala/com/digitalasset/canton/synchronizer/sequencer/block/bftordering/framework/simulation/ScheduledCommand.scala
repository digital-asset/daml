// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.RunningFuture
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Try

import SimulationModuleSystem.SimulationEnv

sealed trait EventOriginator

object EventOriginator {
  case object FromFuture extends EventOriginator
  final case class FromInternalModule(moduleName: ModuleName) extends EventOriginator
  case object FromNetwork extends EventOriginator
  case object FromClient extends EventOriginator
  case object FromInit extends EventOriginator
}

sealed trait ModuleAddress

object ModuleAddress {
  final case class ViaName(moduleName: ModuleName) extends ModuleAddress
  case object Mempool extends ModuleAddress
  case object Output extends ModuleAddress
  case object NetworkIn extends ModuleAddress
  case object NetworkOut extends ModuleAddress
}

sealed trait Command extends Product
final case class InternalEvent[MessageT](
    node: BftNodeId,
    to: ModuleName,
    from: EventOriginator,
    msg: ModuleControl[SimulationEnv, MessageT],
) extends Command
final case class InjectedSend[MessageT](
    node: BftNodeId,
    to: ModuleAddress,
    from: EventOriginator,
    msg: MessageT,
) extends Command
final case class RunFuture[FutureT, MessageT](
    node: BftNodeId,
    to: ModuleName,
    toRun: RunningFuture[FutureT],
    fun: Try[FutureT] => Option[MessageT],
    traceContext: TraceContext,
) extends Command
final case class InternalTick[MessageT](
    node: BftNodeId,
    from: ModuleName,
    tickId: Int,
    msg: ModuleControl[SimulationEnv, MessageT],
) extends Command
final case class ReceiveNetworkMessage[MessageT](
    node: BftNodeId,
    msg: MessageT,
    traceContext: TraceContext,
) extends Command
final case class Quit(reason: String) extends Command
final case class ClientTick[MessageT](
    node: BftNodeId,
    tickId: Int,
    msg: MessageT,
    traceContext: TraceContext,
) extends Command
final case class StartMachine(endpoint: P2PEndpoint) extends Command
final case class PrepareOnboarding(node: BftNodeId) extends Command
final case class AddEndpoint(endpoint: P2PEndpoint, to: BftNodeId) extends Command
final case class EstablishConnection(
    from: BftNodeId,
    to: BftNodeId,
    endpoint: PlainTextP2PEndpoint,
    continuation: (P2PEndpoint.Id, BftNodeId) => Unit,
) extends Command
final case class CrashRestartNode(node: BftNodeId) extends Command
case object MakeSystemHealthy extends Command
case object ResumeLivenessChecks extends Command

final case class ScheduledCommand(
    command: Command,
    at: CantonTimestamp,
    sequenceNumber: Int,
    priority: ScheduledCommand.Priority = ScheduledCommand.DefaultPriority,
)

object ScheduledCommand {

  type Priority = Short

  // The higher the value the higher the priority
  val DefaultPriority: Short = Short.MinValue

  val HighestPriority: Short = Short.MaxValue

  implicit def ordering: Ordering[ScheduledCommand] =
    Ordering.by(command => (-command.at.toMicros, command.priority, -command.sequenceNumber))
}

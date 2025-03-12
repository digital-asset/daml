// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

abstract class Collector[E] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var msgs = Seq.empty[E]
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var tickCounter = 0

  protected def generateNewTickId(): Int = {
    val oldCount = tickCounter
    tickCounter += 1
    oldCount
  }

  def addCancelTick(tickId: Int): Unit

  protected def add(event: E): Unit = blocking(
    // this code should in theory run in a single-threaded deterministic environment, thus there would
    // be no need for using this synchronization mechanism. However currently with the usage of
    // Pekko streams in the OutputModule, this assumption doesn't hold, so we need this for now.
    synchronized {
      msgs = msgs :+ event
    }
  )
  def collect(): Seq[E] = blocking(synchronized {
    val current = msgs
    msgs = Seq.empty[E]
    current
  })
}

class NodeCollector extends Collector[NodeCollector.Event] {
  def addInternalEvent(to: ModuleName, msg: ModuleControl[SimulationEnv, ?]): Unit =
    add(NodeCollector.InternalEvent(to, msg))

  def addTickEvent(
      duration: FiniteDuration,
      to: ModuleName,
      msg: ModuleControl[SimulationEnv, ?],
  ): Int = {
    val newTickId = generateNewTickId()
    add(NodeCollector.TickEvent(duration, newTickId, to, msg))
    newTickId
  }

  def addNetworkEvent(node: BftNodeId, msg: Any)(implicit traceContext: TraceContext): Unit =
    add(NodeCollector.SendNetworkEvent(node, msg, traceContext))

  def addFuture[X, T](
      to: ModuleName,
      future: SimulationFuture[X],
      fun: Try[X] => Option[T],
  )(implicit traceContext: TraceContext): Unit =
    add(NodeCollector.AddFuture(to, future, fun, traceContext))

  def addOpenConnection(
      to: BftNodeId,
      endpoint: PlainTextP2PEndpoint,
      continuation: (P2PEndpoint.Id, BftNodeId) => Unit,
  ): Unit =
    add(NodeCollector.OpenConnection(to, endpoint, continuation))

  override def addCancelTick(tickId: Int): Unit =
    add(NodeCollector.CancelTick(tickId))
}

object NodeCollector {
  sealed trait Event
  final case class InternalEvent(to: ModuleName, msg: ModuleControl[SimulationEnv, ?]) extends Event
  final case class TickEvent(
      duration: FiniteDuration,
      uniqueTickId: Int,
      to: ModuleName,
      msg: ModuleControl[SimulationEnv, ?],
  ) extends Event
  final case class SendNetworkEvent(to: BftNodeId, msg: Any, traceContext: TraceContext)
      extends Event
  final case class AddFuture[X, T](
      to: ModuleName,
      future: SimulationFuture[X],
      fun: Try[X] => Option[T],
      traceContext: TraceContext,
  ) extends Event
  final case class OpenConnection(
      to: BftNodeId,
      endpoint: PlainTextP2PEndpoint,
      continuation: (P2PEndpoint.Id, BftNodeId) => Unit,
  ) extends Event
  final case class CancelTick(tickId: Int) extends Event
}

class ClientCollector(to: ModuleName) extends Collector[ClientCollector.Event] {
  def addTickEvent(duration: FiniteDuration, msg: Any): Int = {
    val tickId = generateNewTickId()
    add(ClientCollector.TickEvent(duration, tickId, msg))
    tickId
  }

  def addClientRequest(msg: Any)(implicit traceContext: TraceContext): Unit = add(
    ClientCollector.ClientRequest(to, msg, traceContext)
  )

  override def addCancelTick(tickId: Int): Unit =
    add(ClientCollector.CancelTick(tickId))
}

object ClientCollector {
  sealed trait Event

  final case class TickEvent(duration: FiniteDuration, tickId: Int, msg: Any) extends Event

  final case class ClientRequest(to: ModuleName, msg: Any, traceContext: TraceContext) extends Event
  final case class CancelTick(tickId: Int) extends Event
}

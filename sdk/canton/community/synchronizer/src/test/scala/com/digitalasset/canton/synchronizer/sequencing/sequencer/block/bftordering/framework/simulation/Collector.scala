// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.topology.SequencerId

import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import SimulationModuleSystem.SimulationEnv

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

  def addNetworkEvent(peer: SequencerId, msg: Any): Unit =
    add(NodeCollector.SendNetworkEvent(peer, msg))

  def addFuture[X, T](to: ModuleName, future: SimulationFuture[X], fun: Try[X] => Option[T]): Unit =
    add(NodeCollector.AddFuture(to, future, fun))

  def addOpenConnection(
      to: SequencerId,
      endpoint: Endpoint,
      continuation: (Endpoint, SequencerId) => Unit,
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
  final case class SendNetworkEvent(to: SequencerId, msg: Any) extends Event
  final case class AddFuture[X, T](
      to: ModuleName,
      future: SimulationFuture[X],
      fun: Try[X] => Option[T],
  ) extends Event
  final case class OpenConnection(
      to: SequencerId,
      endpoint: Endpoint,
      continuation: (Endpoint, SequencerId) => Unit,
  ) extends Event
  final case class CancelTick(tickId: Int) extends Event
}

class ClientCollector(to: ModuleName) extends Collector[ClientCollector.Event] {
  def addTickEvent(duration: FiniteDuration, msg: Any): Int = {
    val tickId = generateNewTickId()
    add(ClientCollector.TickEvent(duration, tickId, msg))
    tickId
  }

  def addClientRequest(msg: Any): Unit = add(
    ClientCollector.ClientRequest(to, msg)
  )

  override def addCancelTick(tickId: Int): Unit =
    add(ClientCollector.CancelTick(tickId))
}

object ClientCollector {
  sealed trait Event

  final case class TickEvent(duration: FiniteDuration, tickId: Int, msg: Any) extends Event

  final case class ClientRequest(to: ModuleName, msg: Any) extends Event
  final case class CancelTick(tickId: Int) extends Event
}

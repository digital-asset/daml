// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.topology.SequencerId

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Random, Try}

import SimulationModuleSystem.SimulationEnv

class LocalSimulator(
    settings: LocalSettings,
    peers: Set[SequencerId],
    agenda: Agenda,
) {
  private val random = new Random(settings.randomSeed)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var canUseFaults = true

  private val crashPeerStatus: mutable.Map[SequencerId, LocalSimulator.CrashPeerStatus] =
    mutable.Map.from(
      peers.map(peer => peer -> LocalSimulator.Uninitialized)
    )

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  def tick(at: CantonTimestamp): Unit = {
    if (!canUseFaults) {
      return
    }
    crashPeerStatus.mapValuesInPlace { case (peer, status) =>
      if (status.shouldUpdate(at)) {
        val gracePeriod =
          at.add(settings.crashRestartGracePeriod.generateRandomDuration(random).toJava)
        if (settings.crashRestartChance.flipCoin(random)) {
          agenda.addOne(CrashRestartPeer(peer), duration = 1.microsecond)
        }
        LocalSimulator.Initialized(gracePeriod)
      } else {
        status
      }
    }
  }

  def scheduleEvent(
      peer: SequencerId,
      to: ModuleName,
      from: EventOriginator,
      msg: ModuleControl[SimulationEnv, ?],
  ): Unit = {
    val (priority, duration) =
      msg match {
        case ModuleControl.Send(_) =>
          /* A guarantee that we need to provide is that if a module `from`` sends message m1 to module `to``
           * and then sends message m2 to the same module, m1 *must* be delivered before m2.
           * So here we find if there is such a message, and uses that as a lower bound for when this message can be
           * scheduled, `msg`` would correspond to m2 in the example.
           */
          val lowFromAgenda = from match {
            case FromInternalModule(fromModuleName) =>
              agenda.findLatestScheduledLocalEvent(peer, fromModuleName, to)
            case _ => None
          }
          ScheduledCommand.DefaultPriority ->
            settings.internalEventTimeDistribution
              .copyWithMaxLow(lowFromAgenda.getOrElse(0.microseconds))
              .generateRandomDuration(random)
        case _ =>
          // Module control messages have the highest priority and are executed immediately
          ScheduledCommand.HighestPriority -> 0.microseconds
      }

    agenda.addOne(
      InternalEvent(peer, to, from, msg),
      duration,
      priority,
    )
  }

  def scheduleTick(
      peer: SequencerId,
      from: ModuleName,
      tickCounter: Int,
      duration: FiniteDuration,
      msg: ModuleControl[SimulationEnv, ?],
  ): Unit = {
    val computedDuration = if (settings.clockDriftChance.flipCoin(random)) {
      duration.plus(settings.clockDrift.generateRandomDuration(random))
    } else duration
    agenda.addOne(
      InternalTick(peer, from, tickCounter, msg),
      computedDuration,
    )
  }

  def scheduleClientTick(
      peer: SequencerId,
      tickCounter: Int,
      duration: FiniteDuration,
      msg: Any,
  ): Unit =
    agenda.addOne(ClientTick(peer, tickCounter, msg), duration)

  def scheduleFuture[X, T](
      peer: SequencerId,
      to: ModuleName,
      now: CantonTimestamp,
      future: SimulationFuture[X],
      fun: Try[X] => Option[T],
  ): Unit = {
    val runningFuture = future.schedule { () =>
      now.add(settings.futureTimeDistribution.generateRandomDuration(random).toJava)
    }

    val timeToRun = runningFuture.minimumScheduledTime.getOrElse(
      now // we can have a future that resolve directly e.g. sequenceFuture(Seq.empty)
    )

    agenda.addOne(
      RunFuture(peer, to, runningFuture, fun),
      timeToRun,
      ScheduledCommand.DefaultPriority,
    )
  }

  def makeHealthy(): Unit =
    canUseFaults = false
}

object LocalSimulator {
  private sealed trait CrashPeerStatus {
    def shouldUpdate(at: CantonTimestamp): Boolean
  }

  private object Uninitialized extends CrashPeerStatus {
    override def shouldUpdate(at: CantonTimestamp): Boolean = true
  }

  private final case class Initialized(dontUpdateUntil: CantonTimestamp) extends CrashPeerStatus {
    override def shouldUpdate(at: CantonTimestamp): Boolean = dontUpdateUntil.isBefore(at)
  }
}

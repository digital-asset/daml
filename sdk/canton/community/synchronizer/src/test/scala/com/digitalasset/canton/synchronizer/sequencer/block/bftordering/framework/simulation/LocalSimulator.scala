// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Random, Try}

import SimulationModuleSystem.SimulationEnv

class LocalSimulator(
    settings: LocalSettings,
    nodes: Set[BftNodeId],
    agenda: Agenda,
) {
  private val random = new Random(settings.randomSeed)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var canUseFaults = true

  private val crashNodeStatus: mutable.Map[BftNodeId, LocalSimulator.CrashNodeStatus] =
    mutable.Map.from(
      nodes.map(node => node -> LocalSimulator.Uninitialized)
    )

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  def tick(at: CantonTimestamp): Unit = {
    if (!canUseFaults) {
      return
    }
    crashNodeStatus.mapValuesInPlace { case (node, status) =>
      if (status.shouldUpdate(at)) {
        val gracePeriod =
          at.add(settings.crashRestartGracePeriod.generateRandomDuration(random).toJava)
        if (settings.crashRestartChance.flipCoin(random)) {
          agenda.addOne(CrashRestartNode(node), duration = 1.microsecond)
        }
        LocalSimulator.Initialized(gracePeriod)
      } else {
        status
      }
    }
  }

  def scheduleEvent(
      node: BftNodeId,
      to: ModuleName,
      from: EventOriginator,
      msg: ModuleControl[SimulationEnv, ?],
  ): Unit = {
    val (priority, duration) =
      msg match {
        case ModuleControl.Send(_, _, _, _, _) =>
          /* A guarantee that we need to provide is that if a module `from`` sends message m1 to module `to``
           * and then sends message m2 to the same module, m1 *must* be delivered before m2.
           * So here we find if there is such a message, and uses that as a lower bound for when this message can be
           * scheduled, `msg`` would correspond to m2 in the example.
           */
          val lowFromAgenda = from match {
            case EventOriginator.FromInternalModule(fromModuleName) =>
              agenda.findLatestScheduledLocalEvent(node, fromModuleName, to)
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
      InternalEvent(node, to, from, msg),
      duration,
      priority,
    )
  }

  def scheduleTick(
      node: BftNodeId,
      from: ModuleName,
      tickCounter: Int,
      duration: FiniteDuration,
      msg: ModuleControl[SimulationEnv, ?],
  ): Unit = {
    val computedDuration = if (settings.clockDriftChance.flipCoin(random)) {
      duration.plus(settings.clockDrift.generateRandomDuration(random))
    } else duration
    agenda.addOne(
      InternalTick(node, from, tickCounter, msg),
      computedDuration,
    )
  }

  def scheduleClientTick(
      node: BftNodeId,
      tickCounter: Int,
      duration: FiniteDuration,
      msg: Any,
      traceContext: TraceContext,
  ): Unit =
    agenda.addOne(ClientTick(node, tickCounter, msg, traceContext), duration)

  def scheduleFuture[X, T](
      node: BftNodeId,
      to: ModuleName,
      now: CantonTimestamp,
      future: SimulationFuture[X],
      fun: Try[X] => Option[T],
      traceContext: TraceContext,
  ): Unit = {
    val runningFuture = future.schedule { () =>
      now.add(settings.futureTimeDistribution.generateRandomDuration(random).toJava)
    }

    val timeToRun = runningFuture.minimumScheduledTime.getOrElse(
      now // we can have a future that resolve directly e.g. sequenceFuture(Seq.empty)
    )

    agenda.addOne(
      RunFuture(node, to, runningFuture, fun, traceContext),
      timeToRun,
      ScheduledCommand.DefaultPriority,
    )
  }

  def makeHealthy(): Unit =
    canUseFaults = false
}

object LocalSimulator {
  private sealed trait CrashNodeStatus {
    def shouldUpdate(at: CantonTimestamp): Boolean
  }

  private object Uninitialized extends CrashNodeStatus {
    override def shouldUpdate(at: CantonTimestamp): Boolean = true
  }

  private final case class Initialized(dontUpdateUntil: CantonTimestamp) extends CrashNodeStatus {
    override def shouldUpdate(at: CantonTimestamp): Boolean = dontUpdateUntil.isBefore(at)
  }
}

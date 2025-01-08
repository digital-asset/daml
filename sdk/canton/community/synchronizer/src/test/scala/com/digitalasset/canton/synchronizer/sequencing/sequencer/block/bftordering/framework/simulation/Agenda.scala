// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

class Agenda(clock: SimClock) {
  implicit private val logger: Logger = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextCommandSequencerNumber = 0

  private val queue = mutable.PriorityQueue.empty[ScheduledCommand]

  def isEmpty: Boolean = queue.isEmpty

  def dequeue(): ScheduledCommand = queue.dequeue()

  private def filterScheduledCommand(
      predicate: ScheduledCommand => Boolean
  ): Unit = {
    val currentElements = queue.dequeueAll
    queue.enqueue(currentElements.filter(predicate)*)
  }

  private def filterCommand(predicate: Command => Boolean): Unit =
    filterScheduledCommand(predicate.compose(_.command))

  def removeCommandsOnCrash(peer: SequencerId): Unit =
    filterScheduledCommand { scheduledCommand =>
      scheduledCommand.command match {
        case InternalEvent(machine, _, _, _) if machine == peer =>
          logger.info(s"Removing internal event $scheduledCommand to simulate crash")
          false
        case RunFuture(machine, _, _, _) if machine == peer =>
          logger.info(s"Removing future from $scheduledCommand to simulate crash")
          false
        case _ => true
      }
    }

  def addOne(
      command: Command,
      duration: FiniteDuration,
      priority: ScheduledCommand.Priority = ScheduledCommand.DefaultPriority,
  ): Unit =
    addOne(command, clock.now.plus(duration.toJava), priority)

  def addOne(
      command: Command,
      at: CantonTimestamp,
      priority: ScheduledCommand.Priority,
  ): Unit = {
    require(at >= clock.now)
    queue.addOne(ScheduledCommand(command, at, nextCommandSequencerNumber, priority))
    nextCommandSequencerNumber += 1
  }

  def removeInternalTick(peer: SequencerId, tickId: Int): Unit = filterCommand {
    case i: InternalTick[_] =>
      i.peer != peer ||
      i.tickId != tickId
    case _ => true
  }

  def findLatestScheduledLocalEvent(
      peer: SequencerId,
      fromModuleName: ModuleName,
      to: ModuleName,
  ): Option[FiniteDuration] =
    queue.toSeq.view.flatMap { scheduledCommand =>
      scheduledCommand.command match {
        case i: InternalEvent[_]
            if peer == i.peer && FromInternalModule(fromModuleName) == i.from && to == i.to =>
          Seq(FiniteDuration((scheduledCommand.at - clock.now).toNanos, TimeUnit.NANOSECONDS))
        case _ => Seq.empty
      }
    }.maxOption

  def removeClientTick(peer: SequencerId, tickId: Int): Unit = filterCommand {
    case i: ClientTick[_] =>
      i.peer != peer ||
      i.tickId != tickId
    case _ => true
  }
}

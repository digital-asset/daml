// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.Agenda.LatestScheduledMessageKey
import com.digitalasset.canton.time.SimClock

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

class Agenda(clock: SimClock, loggerFactory: NamedLoggerFactory) {
  private val logger = loggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextCommandSequencerNumber = 0

  private var queue = mutable.PriorityQueue.empty[ScheduledCommand]

  // Since on each machine, messages from same source to same target are guaranteed to be in order we keep track of the
  // last messages timestamp so we can make sure next ones comes after.
  private val latestScheduledMessageCache =
    mutable.Map.empty[LatestScheduledMessageKey, CantonTimestamp]

  def isEmpty: Boolean = queue.isEmpty

  def dequeue(): ScheduledCommand = queue.dequeue()

  private def filterScheduledCommand(
      predicate: ScheduledCommand => Boolean
  ): Unit =
    queue = queue.filter(predicate)

  private def filterCommand(predicate: Command => Boolean): Unit =
    filterScheduledCommand(predicate.compose(_.command))

  def removeCommandsOnCrash(node: BftNodeId): Unit =
    filterScheduledCommand { scheduledCommand =>
      scheduledCommand.command match {
        case InternalEvent(machine, _, _, _) if machine == node =>
          logger.info(s"Removing internal event $scheduledCommand to simulate crash")
          false
        case InternalTick(machine, _, _, _) if machine == node =>
          logger.info(s"Removing internal tick $scheduledCommand to simulate crash")
          false
        case RunFuture(machine, _, future, _, _) if machine == node =>
          logger.info(s"Removing future ${future.name} from $scheduledCommand to simulate crash")
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
    updateCache(command, at, priority)
    nextCommandSequencerNumber += 1
  }

  def removeInternalTick(node: BftNodeId, tickId: Int): Unit = filterCommand {
    case i: InternalTick[?] =>
      i.node != node ||
      i.tickId != tickId
    case _ => true
  }

  private def updateCache(
      command: Command,
      at: CantonTimestamp,
      priority: ScheduledCommand.Priority,
  ): Unit =
    command match {
      case i: InternalEvent[?] =>
        i.from match {
          case EventOriginator.FromInternalModule(from) =>
            val key = LatestScheduledMessageKey(i.node, from = from, to = i.to)
            latestScheduledMessageCache.updateWith(key) {
              case Some(oldValue) if oldValue.isBefore(at) || oldValue == at => Some(at)
              case Some(oldValue) =>
                require(
                  priority == ScheduledCommand.HighestPriority,
                  "only highest priority is allowed to be scheduled before other commands",
                )
                Some(oldValue)
              case None => Some(at)
            }
          case _ =>
        }
      case _ =>
    }

  def findLatestScheduledLocalEvent(
      node: BftNodeId,
      from: ModuleName,
      to: ModuleName,
  ): Option[FiniteDuration] =
    latestScheduledMessageCache.get(LatestScheduledMessageKey(node, from = from, to = to)) match {
      case Some(scheduledTime) =>
        if (scheduledTime.isBefore(clock.now)) {
          None
        } else {
          Some(FiniteDuration((scheduledTime - clock.now).toNanos, TimeUnit.NANOSECONDS))
        }
      case None => None
    }

  def removeClientTick(node: BftNodeId, tickId: Int): Unit = filterCommand {
    case i: ClientTick[?] =>
      i.node != node ||
      i.tickId != tickId
    case _ => true
  }
}

object Agenda {
  private final case class LatestScheduledMessageKey(
      machine: BftNodeId,
      from: ModuleName,
      to: ModuleName,
  )
}

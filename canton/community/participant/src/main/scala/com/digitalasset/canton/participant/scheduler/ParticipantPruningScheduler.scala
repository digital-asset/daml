// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.scheduler.{PruningSchedule, PruningScheduler, Scheduler, Schedulers}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Extends the Scheduler with flags to configure the pruning scheduler
  */
trait ParticipantPruningScheduler extends PruningScheduler {
  def setParticipantSchedule(schedule: ParticipantPruningSchedule)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def getParticipantSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[ParticipantPruningSchedule]]
}

final case class ParticipantPruningSchedule(
    schedule: PruningSchedule,
    pruneInternallyOnly: Boolean,
) {
  def toProtoV0: v0.ParticipantPruningSchedule = v0.ParticipantPruningSchedule(
    schedule = Some(schedule.toProtoV0),
    pruneInternallyOnly = pruneInternallyOnly,
  )
}

object ParticipantPruningSchedule {
  def fromProtoV0(
      participantSchedule: v0.ParticipantPruningSchedule
  ): ParsingResult[ParticipantPruningSchedule] =
    for {
      scheduleP <- ProtoConverter.required("schedule", participantSchedule.schedule)
      schedule <- PruningSchedule.fromProtoV0(scheduleP)
    } yield ParticipantPruningSchedule(
      schedule,
      participantSchedule.pruneInternallyOnly,
    )

  def fromPruningSchedule(schedule: PruningSchedule): ParticipantPruningSchedule = {
    // By default pruning-internally-only is disabled as "incomplete pruning" of a canton
    // participant needs to be done deliberately in other not to run out of storage space.
    ParticipantPruningSchedule(schedule, pruneInternallyOnly = false)
  }
}

trait SchedulersWithParticipantPruning extends Schedulers {
  def getPruningScheduler(
      loggerFactory: NamedLoggerFactory
  )(implicit traceContext: TraceContext): Option[ParticipantPruningScheduler] = {
    get("pruning") match {
      case Some(ps: ParticipantPruningScheduler) => Some(ps)
      case Some(_) =>
        loggerFactory
          .getTracedLogger(SchedulersWithParticipantPruning.getClass)
          .error("Non-ParticipantPruningScheduler found under \"pruning\". Coding bug.")
        None
      case _ => None
    }
  }

  def setPruningProcessor(pruningProcessor: PruningProcessor): Unit
}

object SchedulersWithParticipantPruning {

  /** No-op schedulers used by the Canton Community edition */
  def noop: SchedulersWithParticipantPruning = new SchedulersWithParticipantPruning {
    override def get(name: String): Option[Scheduler] = None
    override def start()(implicit traceContext: TraceContext): Future[Unit] = Future.unit
    override def stop()(implicit traceContext: TraceContext): Unit = ()
    override def close(): Unit = ()
    override def setPruningProcessor(pruningProcessor: PruningProcessor): Unit = ()
  }
}

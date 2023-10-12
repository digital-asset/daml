// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Trait for a group of schedulers started and stopped as a group.
  */
trait Schedulers extends StartStoppable with AutoCloseable {

  /** Accessor for individual scheduler by name
    * Throws an IllegalStateException if a scheduler is looked up that does not exist
    */
  def get(name: String): Option[Scheduler]
}

trait SchedulersWithPruning extends Schedulers {
  def getPruningScheduler: Option[PruningScheduler] = {
    get("pruning") match {
      case Some(ps: PruningScheduler) => Some(ps)
      case _ => None
    }
  }
}

object SchedulersWithPruning {

  /** No-op schedulers used by the Canton Community edition */
  def noop: SchedulersWithPruning = new SchedulersWithPruning {
    override def get(name: String): Option[Scheduler] = None
    override def start()(implicit traceContext: TraceContext): Future[Unit] = Future.unit
    override def stop()(implicit traceContext: TraceContext): Unit = ()
    override def close(): Unit = ()
  }
}
